#include <iostream>
#include <fstream>
#include <vector>
#include <cstring>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <chrono>
#include <cerrno>
#include <algorithm>
#include "common.h"

int main(int argc, char *argv[])
{
    if (argc < 2)
    {
        std::cout << "Usage: ./client <filepath>\n";
        return 1;
    }

    std::string filepath = argv[1];

    std::ifstream file(filepath, std::ios::binary | std::ios::ate);
    if (!file.is_open())
    {
        std::cerr << "Failed to open file: " << filepath << "\n";
        return 1;
    }

    uint64_t file_size = file.tellg();
    file.seekg(0, std::ios::beg);
    std::vector<char> file_buffer(file_size);
    file.read(file_buffer.data(), file_size);
    file.close();

    uint32_t total_packets = (file_size + PAYLOAD_SIZE - 1) / PAYLOAD_SIZE;
    std::cout << "[Client] File loaded. Size: " << file_size << " bytes, Total Packets: " << total_packets << "\n";

    int sockfd = socket(AF_INET, SOCK_DGRAM, 0);
    sockaddr_in server_addr{};
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(SERVER_PORT);
    inet_pton(AF_INET, SERVER_IP, &server_addr.sin_addr);

    // 设置内核 4MB 缓冲区
    int buf_size = 4 * 1024 * 1024;
    setsockopt(sockfd, SOL_SOCKET, SO_SNDBUF, &buf_size, sizeof(buf_size));
    setsockopt(sockfd, SOL_SOCKET, SO_RCVBUF, &buf_size, sizeof(buf_size));

    // 设置 300ms 接收超时
    struct timeval tv;
    tv.tv_sec = 0;
    tv.tv_usec = 300000; // 300 ms
    setsockopt(sockfd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));

    // 构造并连续发送 5 次 START 包
    PacketHeader start_hdr{};
    start_hdr.type = PKT_START;
    start_hdr.seq = 0;
    start_hdr.total_packets = total_packets;
    start_hdr.file_size = file_size;
    start_hdr.payload_len = 0;

    auto send_start = std::chrono::steady_clock::now();

    for (int i = 0; i < 5; ++i)
    {
        sendto(sockfd, &start_hdr, sizeof(start_hdr), 0,
               (sockaddr *)&server_addr, sizeof(server_addr));
    }

    std::cout << "[Client] START packet sent to " << SERVER_IP << ":" << SERVER_PORT << "\n";

    // 首轮待发送队列为所有包 (0 ~ total_packets - 1)
    std::vector<uint32_t> packets_to_send(total_packets);
    for (uint32_t i = 0; i < total_packets; ++i)
    {
        packets_to_send[i] = i;
    }

    std::vector<char> packet_buf(sizeof(PacketHeader) + PAYLOAD_SIZE);
    std::vector<char> ack_buf(sizeof(PacketHeader) + PAYLOAD_SIZE);

    bool transfer_complete = false;
    int round = 1;

    while (!transfer_complete)
    {
        size_t queue_size = packets_to_send.size();

        // 1. 发送当前轮次队列中的所有数据包
        for (size_t i = 0; i < queue_size; ++i)
        {
            // 【实时计算】：当前轮次还剩下多少个包没有发送
            size_t remaining = queue_size - i;

            int dup_sends = 1;
            if (remaining <= 400)
            {
                dup_sends = 4; // 最后 400 包：4x 喷发，保送首轮/本轮直接收尾
            }
            else if (remaining <= 850)
            {
                dup_sends = 3;
            }
            else if (remaining <= 1700)
            {
                dup_sends = 2; // 进入 BDP 管道容量范围，开始 2x 填充
            }
            else
            {
                dup_sends = 1; // 管道处于满载状态，正常 1x 发送
            }

            uint32_t seq = packets_to_send[i];

            PacketHeader *hdr = reinterpret_cast<PacketHeader *>(packet_buf.data());
            hdr->type = PKT_DATA;
            hdr->seq = seq;
            hdr->total_packets = total_packets;
            hdr->file_size = file_size;

            uint64_t offset = (uint64_t)seq * PAYLOAD_SIZE;
            uint16_t current_len = std::min((uint64_t)PAYLOAD_SIZE, file_size - offset);
            hdr->payload_len = current_len;

            memcpy(packet_buf.data() + sizeof(PacketHeader), file_buffer.data() + offset, current_len);

            // 执行 dynamic dup_sends 次连续喷发
            for (int dup = 0; dup < dup_sends; ++dup)
            {
                sendto(sockfd, packet_buf.data(), sizeof(PacketHeader) + current_len, 0,
                       (sockaddr *)&server_addr, sizeof(server_addr));
            }

            // 精准控速 (100Mbps)
            if (i % 10 == 0)
            {
                usleep(1200);
            }
        }

        std::cout << "[Client] Round " << round << " DATA sent. Sending PKT_FIN and waiting for feedback...\n";

        // 2. 发送本轮结束标记 PKT_FIN (重复 5 次)
        PacketHeader fin_hdr{PKT_FIN, 0, total_packets, file_size, 0};
        for (int i = 0; i < 5; ++i)
        {
            sendto(sockfd, &fin_hdr, sizeof(fin_hdr), 0, (sockaddr *)&server_addr, sizeof(server_addr));
        }

        // 3. 阻塞收集 Server 端返回的反馈
        std::vector<uint8_t> nack_mask(total_packets, 0);
        std::vector<uint32_t> next_round_seqs;
        bool got_response = false;

        while (true)
        {
            ssize_t bytes = recvfrom(sockfd, ack_buf.data(), ack_buf.size(), 0, nullptr, nullptr);

            if (bytes < 0)
            {
                if (errno == EAGAIN || errno == EWOULDBLOCK)
                {
                    if (got_response)
                    {
                        // 已收到部分 NACK 包，超时说明 Server 回发完毕
                        break;
                    }
                    else
                    {
                        // 未收到任何响应，补发 PKT_FIN
                        std::cout << "[Client] Timeout. No response from server, re-sending PKT_FIN...\n";
                        for (int i = 0; i < 5; ++i)
                        {
                            sendto(sockfd, &fin_hdr, sizeof(fin_hdr), 0, (sockaddr *)&server_addr, sizeof(server_addr));
                        }
                        continue;
                    }
                }
                continue;
            }

            if (bytes < (ssize_t)sizeof(PacketHeader))
                continue;

            PacketHeader *ack_hdr = reinterpret_cast<PacketHeader *>(ack_buf.data());

            if (ack_hdr->type == PKT_FIN)
            {
                std::cout << "[Client] Server confirmed PKT_FIN. Transfer complete!\n";
                transfer_complete = true;
                break;
            }
            else if (ack_hdr->type == PKT_NACK)
            {
                got_response = true;
                uint32_t count = ack_hdr->payload_len / sizeof(uint32_t);
                uint32_t *seqs = reinterpret_cast<uint32_t *>(ack_buf.data() + sizeof(PacketHeader));

                for (uint32_t k = 0; k < count; ++k)
                {
                    uint32_t missing_seq = seqs[k];
                    if (missing_seq < total_packets && nack_mask[missing_seq] == 0)
                    {
                        nack_mask[missing_seq] = 1;
                        next_round_seqs.push_back(missing_seq);
                    }
                }
            }
        }

        if (transfer_complete)
            break;

        packets_to_send = std::move(next_round_seqs);
        std::cout << "[Client] Round " << round << " ended. Retransmitting "
                  << packets_to_send.size() << " missing packets in next round.\n";
        round++;
    }

    auto send_end = std::chrono::steady_clock::now();

    double send_sec = std::chrono::duration<double>(send_end - send_start).count();
    double send_rate_mbps = file_size * 8.0 / send_sec / 1000000.0;

    std::cout << "======================================\n";
    std::cout << "Total transmission time: " << send_sec << " sec\n";
    std::cout << "Average transmission rate: " << send_rate_mbps << " Mbits/sec\n";

    close(sockfd);
    return 0;
}