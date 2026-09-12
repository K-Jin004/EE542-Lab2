#include <iostream>
#include <fstream>
#include <vector>
#include <cstring>
#include <sys/socket.h>
#include <netinet/in.h>
#include <unistd.h>
#include "common.h"

int main(int argc, char *argv[])
{
    if (argc < 2)
    {
        std::cout << "Usage: ./server <output_filepath>\n";
        return 1;
    }
    std::string output_path = argv[1];

    int sockfd = socket(AF_INET, SOCK_DGRAM, 0);

    sockaddr_in server_addr{};
    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = INADDR_ANY; // 监听所有本地网卡
    server_addr.sin_port = htons(SERVER_PORT);

    if (bind(sockfd, (sockaddr *)&server_addr, sizeof(server_addr)) < 0)
    {
        std::cerr << "Bind failed!\n";
        return 1;
    }

    int buf_size = 4 * 1024 * 1024;
    setsockopt(sockfd, SOL_SOCKET, SO_RCVBUF, &buf_size, sizeof(buf_size));
    setsockopt(sockfd, SOL_SOCKET, SO_SNDBUF, &buf_size, sizeof(buf_size));

    std::cout << "[Server] Waiting for START packet on " << SERVER_IP << ":" << SERVER_PORT << "...\n";

    std::vector<char> recv_buf(sizeof(PacketHeader) + PAYLOAD_SIZE);
    std::vector<char> file_buffer; // 接收文件的 DRAM 缓冲区

    std::vector<uint8_t> received_map; // bitmap for ack

    uint32_t total_packets = 0;
    uint64_t file_size = 0;
    uint32_t received_packets = 0;
    bool initialized = false;
    uint32_t last_round = 0;

    sockaddr_in client_addr{};
    socklen_t addr_len = sizeof(client_addr);

    while (true)
    {
        ssize_t bytes = recvfrom(sockfd, recv_buf.data(), recv_buf.size(), 0, (sockaddr *)&client_addr, &addr_len);

        if (bytes < (ssize_t)sizeof(PacketHeader))
            continue;

        PacketHeader *hdr = reinterpret_cast<PacketHeader *>(recv_buf.data());

        if (hdr->type == PKT_START && !initialized)
        {

            total_packets = hdr->total_packets;
            file_size = hdr->file_size;
            file_buffer.resize(file_size);
            received_map.assign(total_packets, 0);
            initialized = true;

            std::cout << "[Server] START received. Allocated " << file_size << " bytes in DRAM.\n";
            std::cout << "[Server] Total Packets: " << hdr->total_packets << "\n";
            std::cout << "======================================\n";
        }
        else if (hdr->type == PKT_DATA && initialized)
        {
            uint32_t seq = hdr->seq;
            uint64_t offset = (uint64_t)seq * PAYLOAD_SIZE;

            if (received_map[seq] == 0)
            {
                
                // 将负载数据直接 memcpy 到 DRAM 缓冲区的正确偏移处
                memcpy(file_buffer.data() + offset,
                       recv_buf.data() + sizeof(PacketHeader),
                       hdr->payload_len);

                received_map[seq] = 1;
                received_packets++;

                if (received_packets % 10000 == 0 || received_packets == total_packets)
                {
                    std::cout << "[Server] Progress: " << received_packets << "/" << total_packets << " packets.\n";
                }

                if (received_packets == total_packets)
                {
                    std::cout << "[Server] All packets received in DRAM!\n";
                }
            }
        }
        else if (hdr->type == PKT_FIN && initialized)
        {
            

            // skip duplicate FIN
            if (hdr->round <= last_round) {
                continue;
            }

            std::cout << "[Server] Received PKT_FIN from Client for Round " << hdr->round << "\n";

            // 检测数据是否 100% 完整
            if (received_packets == total_packets)
            {
                std::cout << "[Server] All packets verified! Saving file...\n";

                std::ofstream out_file(output_path, std::ios::binary);
                out_file.write(file_buffer.data(), file_size);
                out_file.close();

                // 给 Client 回发确认 PKT_FIN
                PacketHeader fin_hdr{PKT_FIN, 0, total_packets, file_size, 0};
                for (int i = 0; i < 5; ++i)
                {
                    sendto(sockfd, &fin_hdr, sizeof(fin_hdr), 0, (sockaddr *)&client_addr, addr_len);
                }

                std::cout << "[Server] Transmission successfully finished!\n";
                break;
            }
            else
            {
                last_round = hdr->round;

                std::cout << "[Server] Client sent FIN, but missing "
                          << (total_packets - received_packets) << " packets.\n";
                
                //线性扫描 Bitmap 提取缺失序号
                std::vector<uint32_t> missing_seqs;
                missing_seqs.reserve(total_packets - received_packets);
                for (uint32_t i = 0; i < total_packets; ++i)
                {
                    if (received_map[i] == 0)
                    {
                        missing_seqs.push_back(i);
                    }
                }

                const size_t max_seqs_per_pkt = PAYLOAD_SIZE / sizeof(uint32_t);
                std::vector<char> nack_pkt_buf(sizeof(PacketHeader) + PAYLOAD_SIZE);
                uint32_t sent_nack_pkts = 0;

                for (int redundancy = 0; redundancy < 3; ++redundancy)
                {
                    for (size_t offset = 0; offset < missing_seqs.size(); offset += max_seqs_per_pkt)
                    {
                        uint32_t count = std::min((size_t)max_seqs_per_pkt, missing_seqs.size() - offset);
                        uint32_t payload_bytes = count * sizeof(uint32_t);

                        PacketHeader *nack_hdr = reinterpret_cast<PacketHeader *>(nack_pkt_buf.data());
                        nack_hdr->type = PKT_NACK;
                        nack_hdr->seq = offset / max_seqs_per_pkt; // 记载分包批次序号
                        nack_hdr->payload_len = payload_bytes;

                        memcpy(nack_pkt_buf.data() + sizeof(PacketHeader),
                               &missing_seqs[offset],
                               payload_bytes);

                        sendto(sockfd, nack_pkt_buf.data(), sizeof(PacketHeader) + payload_bytes, 0,
                               (sockaddr *)&client_addr, addr_len);

                        sent_nack_pkts++;
                        if (sent_nack_pkts %10 == 0) {
                            usleep(1000);
                        }
                    }
                }

                std::cout << "[Server] Sent " << missing_seqs.size() 
                          << " missing seqs in NACK packets (3x redundancy).\n";
                std::cout << "======================================\n";
            }
        }
    }

    close(sockfd);
    return 0;
}