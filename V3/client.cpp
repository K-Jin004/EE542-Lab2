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
#include <thread>
#include <atomic>
#include "common.h"

// 设置发送并发线程数（与 Server RX 线程数相匹配）
constexpr int NUM_TX_THREADS = NUM_RX_THREADS;

void tx_worker_thread(int thread_idx,
                      const std::vector<char> &file_buffer, uint64_t file_size,
                      uint32_t total_packets, uint32_t current_round,
                      const std::vector<uint32_t> &packets_to_send,
                      std::atomic<size_t> &global_idx)
{
    // 每个发送线程独立创建套接字
    int worker_sockfd = socket(AF_INET, SOCK_DGRAM, 0);
    if (worker_sockfd < 0)
    {
        perror("Worker socket creation failed");
        return;
    }

    // 设置线程专属的内核 4MB 发送缓冲区
    int buf_size = 4 * 1024 * 1024;
    setsockopt(worker_sockfd, SOL_SOCKET, SO_SNDBUF, &buf_size, sizeof(buf_size));

    // 计算对应的 Server 端数据接收端口 (8081, 8082, 8083, 8084)
    uint16_t target_port = SERVER_BASE_PORT + thread_idx;
    sockaddr_in server_data_addr{};
    server_data_addr.sin_family = AF_INET;
    server_data_addr.sin_port = htons(target_port);
    inet_pton(AF_INET, SERVER_IP, &server_data_addr.sin_addr);



    size_t queue_size = packets_to_send.size();
    std::vector<char> packet_buf(sizeof(PacketHeader) + PAYLOAD_SIZE);

    while (true)
    {
        // 无锁原子竞争获取下一个待发送数据包索引
        size_t i = global_idx.fetch_add(1, std::memory_order_relaxed);
        if (i >= queue_size)
            break;

        // 动态尾部重发喷发控制
        size_t remaining = queue_size - i;
        int dup_sends = 1;
        if (remaining <= 400)
        {
            dup_sends = 4;
        }
        else if (remaining <= 850)
        {
            dup_sends = 3;
        }
        else if (remaining <= 1700)
        {
            dup_sends = 2;
        }
        else
        {
            dup_sends = 1;
        }
        uint32_t seq = packets_to_send[i];

        PacketHeader *hdr = reinterpret_cast<PacketHeader *>(packet_buf.data());
        hdr->type = PKT_DATA;
        hdr->seq = seq;
        hdr->total_packets = total_packets;
        hdr->file_size = file_size;
        hdr->round = current_round;

        uint64_t offset = (uint64_t)seq * PAYLOAD_SIZE;
        uint16_t current_len = std::min((uint64_t)PAYLOAD_SIZE, file_size - offset);
        hdr->payload_len = current_len;

        memcpy(packet_buf.data() + sizeof(PacketHeader), file_buffer.data() + offset, current_len);

        // 使用本线程独立的 Socket 发送给 Server 对应的专有端口
        for (int dup = 0; dup < dup_sends; ++dup)
        {
            sendto(worker_sockfd, packet_buf.data(), sizeof(PacketHeader) + current_len, 0,
                   (sockaddr *)&server_data_addr, sizeof(server_data_addr));
            
            usleep(350);
        }

    
        
        
    }

    close(worker_sockfd);
}

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

    // 1. 创建控制套接字，对接 Server 的 SERVER_MAIN_PORT (8080)
    int control_sockfd = socket(AF_INET, SOCK_DGRAM, 0);
    sockaddr_in server_main_addr{};
    server_main_addr.sin_family = AF_INET;
    server_main_addr.sin_port = htons(SERVER_MAIN_PORT);
    inet_pton(AF_INET, SERVER_IP, &server_main_addr.sin_addr);

    // 设置控制 Socket 缓冲区
    int buf_size = 4 * 1024 * 1024;
    setsockopt(control_sockfd, SOL_SOCKET, SO_SNDBUF, &buf_size, sizeof(buf_size));
    setsockopt(control_sockfd, SOL_SOCKET, SO_RCVBUF, &buf_size, sizeof(buf_size));

    // 设置 300ms 接收超时
    struct timeval tv;
    tv.tv_sec = 0;
    tv.tv_usec = 300000; // 300 ms
    setsockopt(control_sockfd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));

    // 2. 向 Server 主端口发送 START 包 (重复 5 次)
    PacketHeader start_hdr{};
    start_hdr.type = PKT_START;
    start_hdr.seq = 0;
    start_hdr.total_packets = total_packets;
    start_hdr.file_size = file_size;
    start_hdr.payload_len = 0;

    auto send_start = std::chrono::steady_clock::now();

    for (int i = 0; i < 5; ++i)
    {
        sendto(control_sockfd, &start_hdr, sizeof(start_hdr), 0,
               (sockaddr *)&server_main_addr, sizeof(server_main_addr));
    }

    std::cout << "[Client] START packet sent to control port " << SERVER_IP << ":" << SERVER_MAIN_PORT << "\n";

    // 首轮待发送队列为所有包 (0 ~ total_packets - 1)
    std::vector<uint32_t> packets_to_send(total_packets);
    for (uint32_t i = 0; i < total_packets; ++i)
    {
        packets_to_send[i] = i;
    }

    std::vector<char> ack_buf(sizeof(PacketHeader) + PAYLOAD_SIZE);

    bool transfer_complete = false;
    uint32_t current_round = 1;

    while (!transfer_complete)
    {
        size_t queue_size = packets_to_send.size();

        // 3. 创建 NUM_TX_THREADS 个并发发送线程，每个线程拥有独立的 Socket 并对接独立的 Server 数据端口
        std::atomic<size_t> global_tx_idx{0};
        std::vector<std::thread> tx_threads;
        tx_threads.reserve(NUM_TX_THREADS);

        for (int t = 0; t < NUM_TX_THREADS; ++t)
        {
            tx_threads.emplace_back(tx_worker_thread, t,
                                    std::ref(file_buffer), file_size, total_packets,
                                    current_round, std::ref(packets_to_send),
                                    std::ref(global_tx_idx));
        }

        // 等待本轮并发数据喷发结束
        for (auto &t : tx_threads)
        {
            if (t.joinable())
                t.join();
        }

        std::cout << "[Client] Round " << current_round << " DATA sent (" << NUM_TX_THREADS
                  << " sockets): " << queue_size << " packets. Sending PKT_FIN to control port...\n";

        // 4. 向 Server 主端口发送本轮结束标记 PKT_FIN (重复 5 次)
        PacketHeader fin_hdr{};
        fin_hdr.type = PKT_FIN;
        fin_hdr.seq = 0;
        fin_hdr.total_packets = total_packets;
        fin_hdr.file_size = file_size;
        fin_hdr.round = current_round;
        fin_hdr.payload_len = 0;

        for (int i = 0; i < 5; ++i)
        {
            sendto(control_sockfd, &fin_hdr, sizeof(fin_hdr), 0,
                   (sockaddr *)&server_main_addr, sizeof(server_main_addr));
        }

        // 5. 控制 Socket 阻塞接收 Server 控制线程反馈 (NACK / FIN)
        std::vector<uint8_t> nack_mask(total_packets, 0);
        std::vector<uint8_t> received_nack_pkts(total_packets, 0);

        std::vector<uint32_t> next_round_seqs;
        bool got_response = false;

        while (true)
        {
            ssize_t bytes = recvfrom(control_sockfd, ack_buf.data(), ack_buf.size(), 0, nullptr, nullptr);

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
                            sendto(control_sockfd, &fin_hdr, sizeof(fin_hdr), 0,
                                   (sockaddr *)&server_main_addr, sizeof(server_main_addr));
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
                uint32_t nack_pkt_id = ack_hdr->seq;

                // 过滤重复的 NACK 包
                if (nack_pkt_id < total_packets && received_nack_pkts[nack_pkt_id] == 1)
                {
                    continue;
                }

                if (nack_pkt_id < total_packets)
                {
                    received_nack_pkts[nack_pkt_id] = 1;
                }

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
        std::cout << "[Client] Round " << current_round << " ended. Retransmitting "
                  << packets_to_send.size() << " missing packets in next round.\n";

        std::cout << "======================================\n";
        current_round++;
    }

    auto send_end = std::chrono::steady_clock::now();

    double send_sec = std::chrono::duration<double>(send_end - send_start).count();
    double send_rate_mbps = file_size * 8.0 / send_sec / 1000000.0;

    std::cout << "======================================\n";
    std::cout << "Total transmission time: " << send_sec << " sec\n";
    std::cout << "Average transmission rate: " << send_rate_mbps << " Mbits/sec\n";

    close(control_sockfd);
    return 0;
}