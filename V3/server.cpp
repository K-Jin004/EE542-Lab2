#include <iostream>
#include <vector>
#include <atomic>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <sys/socket.h>
#include <netinet/in.h>
#include <unistd.h>
#include <fstream>
#include <memory>
#include <algorithm>
#include <cstring>
#include "common.h"

struct ServerState
{
    uint32_t total_packets = 0;
    uint64_t file_size = 0;
    std::atomic<uint32_t> received_packets{0};
    std::atomic<bool> initialized{false};
    std::mutex init_mtx; // 保护初始化阶段的并发竞争

    std::vector<char> file_buffer;
    // 使用 std::unique_ptr 管理动态 atomic 数组
    std::unique_ptr<std::atomic<uint8_t>[]> received_map;

    // 事件通知：RX 线程通知 Control 线程收到了 PKT_FIN
    std::mutex fin_mtx;
    std::condition_variable fin_cv;
    std::atomic<uint32_t> current_fin_round{0};
    std::atomic<bool> has_pending_fin{false};

    sockaddr_in client_addr{};
    socklen_t addr_len = sizeof(client_addr);
};

ServerState g_state;

void rx_thread_func(int thread_idx, int listen_fd)
{
    std::vector<char> recv_buf(sizeof(PacketHeader) + PAYLOAD_SIZE);

    while (true)
    {
        sockaddr_in client_addr{};
        socklen_t addr_len = sizeof(client_addr);

        ssize_t bytes = recvfrom(listen_fd, recv_buf.data(), recv_buf.size(), 0,
                                 (sockaddr *)&client_addr, &addr_len);

        if (bytes < (ssize_t)sizeof(PacketHeader))
            continue;

        PacketHeader *hdr = reinterpret_cast<PacketHeader *>(recv_buf.data());

        // 1. 处理 START 包 (加锁双重检查，防止多线程同时收到 START 重复初始化)
        if (hdr->type == PKT_START && !g_state.initialized.load(std::memory_order_relaxed))
        {
            std::lock_guard<std::mutex> lock(g_state.init_mtx);
            if (!g_state.initialized.load())
            {
                g_state.total_packets = hdr->total_packets;
                g_state.file_size = hdr->file_size;
                g_state.file_buffer.resize(hdr->file_size);

                g_state.received_map = std::make_unique<std::atomic<uint8_t>[]>(hdr->total_packets);
                for (uint32_t i = 0; i < hdr->total_packets; ++i)
                {
                    g_state.received_map[i].store(0, std::memory_order_relaxed);
                }

                g_state.client_addr = client_addr;
                g_state.addr_len = addr_len;
                g_state.initialized.store(true);
                std::cout << "[Server] Global State Initialized on RX Thread " << thread_idx
                          << ". Total Packets: " << hdr->total_packets
                          << ", File Size: " << hdr->file_size << " bytes.\n";
            }
        }
        // 2. 处理 DATA 包 (彻底无锁写入 DRAM)
        else if (hdr->type == PKT_DATA && g_state.initialized.load(std::memory_order_relaxed))
        {
            uint32_t seq = hdr->seq;
            if (seq < g_state.total_packets)
            {
                uint8_t expected = 0;
                // 原子 CAS 操作：仅首次收到的包执行 memcpy 和计数
                if (g_state.received_map[seq].compare_exchange_strong(expected, 1, std::memory_order_relaxed))
                {
                    uint64_t offset = (uint64_t)seq * PAYLOAD_SIZE;
                    memcpy(g_state.file_buffer.data() + offset,
                           recv_buf.data() + sizeof(PacketHeader),
                           hdr->payload_len);

                    uint32_t curr_cnt = g_state.received_packets.fetch_add(1, std::memory_order_relaxed) + 1;
                    if (curr_cnt % 50000 == 0 || curr_cnt == g_state.total_packets)
                    {
                        std::cout << "[Server] Progress: " << curr_cnt << "/" << g_state.total_packets << " packets.\n";
                    }
                }
            }
        }
        // 3. 处理 FIN 包 (唤醒 Control 线程，RX 线程立刻返回继续收包)
        else if (hdr->type == PKT_FIN && g_state.initialized.load(std::memory_order_relaxed))
        {
            {
                std::lock_guard<std::mutex> lock(g_state.fin_mtx);
                g_state.current_fin_round.store(hdr->round);
                g_state.has_pending_fin.store(true);
            }
            g_state.fin_cv.notify_one();
        }
    }
}

void control_thread_func(int control_fd, std::string output_path)
{
    uint32_t last_processed_round = 0;
    std::vector<char> recv_buf(sizeof(PacketHeader) + PAYLOAD_SIZE);

    while (true)
    {
        sockaddr_in client_addr{};
        socklen_t addr_len = sizeof(client_addr);

        // 阻塞接收控制端口 (8080) 的 PKT_START 和 PKT_FIN
        ssize_t bytes = recvfrom(control_fd, recv_buf.data(), recv_buf.size(), 0,
                                 (sockaddr *)&client_addr, &addr_len);

        if (bytes < (ssize_t)sizeof(PacketHeader))
            continue;

        PacketHeader *hdr = reinterpret_cast<PacketHeader *>(recv_buf.data());

        // 1. 在控制端口处理 PKT_START 初始化
        if (hdr->type == PKT_START && !g_state.initialized.load(std::memory_order_relaxed))
        {
            std::lock_guard<std::mutex> lock(g_state.init_mtx);
            if (!g_state.initialized.load())
            {
                g_state.total_packets = hdr->total_packets;
                g_state.file_size = hdr->file_size;
                g_state.file_buffer.resize(hdr->file_size);

                g_state.received_map = std::make_unique<std::atomic<uint8_t>[]>(hdr->total_packets);
                for (uint32_t i = 0; i < hdr->total_packets; ++i)
                {
                    g_state.received_map[i].store(0, std::memory_order_relaxed);
                }

                g_state.client_addr = client_addr;
                g_state.addr_len = addr_len;
                g_state.initialized.store(true);
                std::cout << "[Server] Global State Initialized on Control Port " << SERVER_MAIN_PORT
                          << ". Total Packets: " << hdr->total_packets
                          << ", File Size: " << hdr->file_size << " bytes.\n";
            }
        }
        // 2. 在控制端口处理 PKT_FIN
        else if (hdr->type == PKT_FIN && g_state.initialized.load(std::memory_order_relaxed))
        {
            uint32_t round = hdr->round;
            if (round <= last_processed_round)
                continue;
            last_processed_round = round;

            // 更新 Client 地址，确保控制回复发往最新客户端 IP/Port
            g_state.client_addr = client_addr;
            g_state.addr_len = addr_len;

            uint32_t recv_cnt = g_state.received_packets.load(std::memory_order_relaxed);

            // A. 全部收齐 -> 回发 PKT_FIN 确认，启动异步落盘
            if (recv_cnt == g_state.total_packets)
            {
                std::cout << "[Server] All packets verified! Sending final ACK and saving file...\n";

                PacketHeader fin_hdr{PKT_FIN, 0, g_state.total_packets, g_state.file_size, 0, round};
                for (int i = 0; i < 5; ++i)
                {
                    sendto(control_fd, &fin_hdr, sizeof(fin_hdr), 0,
                           (sockaddr *)&g_state.client_addr, g_state.addr_len);
                }

                std::thread save_thread([output_path]()
                {
                    std::ofstream out_file(output_path, std::ios::binary);
                    out_file.write(g_state.file_buffer.data(), g_state.file_size);
                    out_file.close();
                    std::cout << "[Server] File successfully written to disk: " << output_path << "\n"; 
                });
                save_thread.detach();
                break;
            }
            // B. 存在缺失 -> 扫描 Bitmap 打包回发 NACK
            else
            {
                std::cout << "[Server] Round " << round << " FIN received, missing "
                          << (g_state.total_packets - recv_cnt) << " packets. Sending NACKs...\n";

                std::vector<uint32_t> missing_seqs;
                missing_seqs.reserve(g_state.total_packets - recv_cnt);

                for (uint32_t i = 0; i < g_state.total_packets; ++i)
                {
                    if (g_state.received_map[i].load(std::memory_order_relaxed) == 0)
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
                        nack_hdr->seq = offset / max_seqs_per_pkt;
                        nack_hdr->payload_len = payload_bytes;

                        memcpy(nack_pkt_buf.data() + sizeof(PacketHeader),
                               &missing_seqs[offset],
                               payload_bytes);

                        sendto(control_fd, nack_pkt_buf.data(), sizeof(PacketHeader) + payload_bytes, 0,
                               (sockaddr *)&g_state.client_addr, g_state.addr_len);

                        sent_nack_pkts++;
                        if (sent_nack_pkts % 10 == 0)
                        {
                            usleep(800);
                        }
                    }
                }
                std::cout << "[Server] Sent " << missing_seqs.size() 
                          << " missing seqs in NACK packets (3x redundancy, total " 
                          << sent_nack_pkts << " pkts sent).\n";
                std::cout << "======================================\n";
            }
        }
    }
}

int create_udp_socket(int port)
{
    int fd = socket(AF_INET, SOCK_DGRAM, 0);
    if (fd < 0)
    {
        perror("Socket creation failed");
        exit(EXIT_FAILURE);
    }

    // 为独立 Socket 分配更大的内核缓冲区 (8MB)
    int buf_size = 8 * 1024 * 1024;
    setsockopt(fd, SOL_SOCKET, SO_RCVBUF, &buf_size, sizeof(buf_size));
    setsockopt(fd, SOL_SOCKET, SO_SNDBUF, &buf_size, sizeof(buf_size));

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons(port);

    if (bind(fd, (sockaddr *)&addr, sizeof(addr)) < 0)
    {
        perror("Bind failed");
        exit(EXIT_FAILURE);
    }
    return fd;
}

int main(int argc, char *argv[])
{
    if (argc < 2)
    {
        std::cout << "Usage: ./server <output_filepath>\n";
        return 1;
    }
    std::string output_path = argv[1];

    std::cout << "[Server] Starting multi-socket UDP Server...\n";

    // 1. 创建专用的控制端口套接字 (Main Port 8080)
    int control_fd = create_udp_socket(SERVER_MAIN_PORT);
    std::cout << "[Server] Control socket listening on port " << SERVER_MAIN_PORT << "\n";

    // 2. 为每个 RX 数据接收线程创建独立的套接字与物理端口 (Ports 8081 ~ 8084)
    std::vector<std::thread> rx_threads;
    std::vector<int> rx_fds;

    for (size_t i = 0; i < NUM_RX_THREADS; ++i)
    {
        int port = SERVER_BASE_PORT + i;
        int fd = create_udp_socket(port);
        rx_fds.push_back(fd);
        rx_threads.emplace_back(rx_thread_func, i, fd);
        std::cout << "[Server] Worker Thread " << i << " listening on port " << port << "\n";
    }

    // 3. 启动独立控制线程（NACK/FIN 发送）
    // 在 main 函数中直接启动更新后的控制线程
    std::thread control_thread(control_thread_func, control_fd, output_path);

    // 等待控制线程处理完毕退出
    control_thread.join();

    // 清理资源
    for (auto &t : rx_threads)
    {
        if (t.joinable())
            t.detach();
    }
    for (int fd : rx_fds)
    {
        close(fd);
    }
    close(control_fd);

    std::cout << "[Server] Transfer finished cleanly.\n";
    return 0;
}