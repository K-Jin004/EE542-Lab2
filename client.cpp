#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>
#include <fcntl.h>

#include <chrono>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <iostream>
#include <vector>
#include <deque>

const int DEFAULT_CHUNK_SIZE = 1400;
const int DEFAULT_WINDOW_SIZE = 128;
const int DEFAULT_TIMEOUT_MS = 50;

const size_t RING_SIZE = 16384;
const uint64_t PRINT_INTERVAL = 10ULL * 1024 * 1024;

enum PacketType
{
    DATA = 1,
    ACK = 2,
    FIN = 3,
    FIN_ACK = 4
};

struct PacketHeader
{
    uint32_t type;
    uint32_t seq;
    uint32_t sack_seq;
    uint32_t length;
};

struct PacketState
{
    PacketHeader header;
    char payload[DEFAULT_CHUNK_SIZE];
    bool acked;
    std::chrono::steady_clock::time_point last_sent;
};

void send_data_packet(int sockfd, sockaddr_in &server_addr, PacketState &pkt)
{
    char buffer[sizeof(PacketHeader) + 1400];

    std::memcpy(buffer, &pkt.header, sizeof(PacketHeader));
    std::memcpy(buffer + sizeof(PacketHeader), pkt.payload, pkt.header.length);

    sendto(sockfd,
           buffer,
           sizeof(PacketHeader) + pkt.header.length,
           0,
           reinterpret_cast<sockaddr *>(&server_addr),
           sizeof(server_addr));
}

bool send_fin_wait_ack(int sockfd, sockaddr_in &server_addr, uint32_t fin_seq, uint64_t &fin_sent)
{
    PacketHeader fin{};
    fin.type = FIN;
    fin.seq = fin_seq;
    fin.sack_seq = fin_seq;
    fin.length = 0;

    while (true)
    {
        // 发送 FIN
        sendto(sockfd, &fin, sizeof(fin), 0, reinterpret_cast<sockaddr *>(&server_addr), sizeof(server_addr));
        fin_sent++;

        auto start = std::chrono::steady_clock::now();
        while (true)
        {
            PacketHeader ack{};
            ssize_t n = recvfrom(sockfd, &ack, sizeof(ack), 0, nullptr, nullptr);

            if (n >= static_cast<ssize_t>(sizeof(PacketHeader)) &&
                ack.type == FIN_ACK &&
                ack.seq == fin_seq)
            {
                std::cout << "FIN acknowledged\n";
                return true;
            }

            // 检查是否等待满 300ms 超时
            auto now = std::chrono::steady_clock::now();
            auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(now - start).count();
            if (elapsed > 300)
            {
                std::cout << "FIN ACK timeout, resend FIN\n";
                break; // 超时，跳出内层循环重新 sendto
            }

            usleep(1000); // 1ms 休眠，防止 CPU 空转占满
        }
    }
}


int main(int argc, char *argv[])
{
    if (argc < 4 || argc > 7)
    {
        std::cerr << "usage: " << argv[0]
                  << " <server_ip> <server_port> <input_file> "
                  << "[chunk_size] [window_size] [timeout_ms]\n";
        return 1;
    }

    const char *server_ip = argv[1];
    int server_port = std::atoi(argv[2]);
    const char *input_file = argv[3];

    int chunk_size = DEFAULT_CHUNK_SIZE;
    int window_size = DEFAULT_WINDOW_SIZE;
    int timeout_ms = DEFAULT_TIMEOUT_MS;

    if (argc >= 5)
    {
        chunk_size = std::atoi(argv[4]);
    }

    if (argc >= 6)
    {
        window_size = std::atoi(argv[5]);
    }

    if (argc >= 7)
    {
        timeout_ms = std::atoi(argv[6]);
    }

    std::cout << "chunk_size: " << chunk_size << " bytes\n";
    std::cout << "window_size: " << window_size << " packets\n";
    std::cout << "timeout: " << timeout_ms << " ms\n";

    int sockfd = socket(AF_INET, SOCK_DGRAM, 0);
    if (sockfd < 0)
    {
        perror("socket");
        return 1;
    }


    // 替换为：设置为非阻塞模式 nonblocking
    /*
    int flags = fcntl(sockfd, F_GETFL, 0);
    if (fcntl(sockfd, F_SETFL, flags | O_NONBLOCK) < 0)
    {
        perror("fcntl O_NONBLOCK");
        close(sockfd);
        return 1;
    }
    */
    

    //

    sockaddr_in server_addr{};
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(server_port);

    if (inet_pton(AF_INET, server_ip, &server_addr.sin_addr) != 1)
    {
        std::cerr << "invalid server ip\n";
        close(sockfd);
        return 1;
    }

    std::ifstream in(input_file, std::ios::binary);
    if (!in)
    {
        std::cerr << "cannot open input file\n";
        close(sockfd);
        return 1;
    }

    // 设置内核 4MB 缓冲区
    int buf_size = 4 * 1024 * 1024;
    setsockopt(sockfd, SOL_SOCKET, SO_SNDBUF, &buf_size, sizeof(buf_size));
    setsockopt(sockfd, SOL_SOCKET, SO_RCVBUF, &buf_size, sizeof(buf_size));

    std::vector<PacketState> window(RING_SIZE);
    uint32_t base_seq = 0; // 窗口队头 seq
    uint32_t next_seq = 0; // 下一个发包 seq
    uint64_t total_sent = 0;
    bool file_done = false;

    uint64_t next_print = PRINT_INTERVAL;

    // ———— Statistics
    uint64_t data_packet_sent = 0;   // 第一次发送 DATA 的数量
    uint64_t data_packet_resent = 0; // 重发 DATA 的数量
    uint64_t ack_received = 0;       // 收到 ACK 的数量
    uint64_t timeout_count = 0;      // timeout 导致重发的次数
    uint64_t fin_sent = 0;           // FIN 发送次数

    static int pkt_count = 0;
    //---

    auto start_time = std::chrono::steady_clock::now();

    while (!file_done || base_seq < next_seq)
    {
        // 填充滑动窗口
        while (!file_done && (next_seq - base_seq) < static_cast<uint32_t>(window_size))
        {
            auto &pkt = window[next_seq % RING_SIZE];

            in.read(pkt.payload, chunk_size);
            std::streamsize bytes_read = in.gcount();

            if (bytes_read <= 0)
            {
                file_done = true;
                break;
            }

            pkt.header.type = DATA;
            pkt.header.seq = next_seq;
            pkt.header.length = bytes_read;
            pkt.acked = false;

            send_data_packet(sockfd, server_addr, pkt);
            pkt.last_sent = std::chrono::steady_clock::now();
            data_packet_sent++;

            total_sent += bytes_read;
            pkt_count++;

            if (pkt_count % 4 == 0)
            {
                usleep(400);
            }

            if (total_sent >= next_print)
            {
                std::cout << "sent " << (total_sent / (1024 * 1024)) << " MB\n";
                next_print += PRINT_INTERVAL;
            }

            next_seq++;
        }

        PacketHeader ack{};
        while (true)
        {
            ssize_t n = recvfrom(sockfd, &ack, sizeof(ack), MSG_DONTWAIT, nullptr, nullptr);
            if (n <= 0)
            {
                break; // 缓冲区已空，跳出
            }
            if (n >= static_cast<ssize_t>(sizeof(PacketHeader)) && ack.type == ACK)
            {
                ack_received++;
                if (base_seq < next_seq)
                {
                    // 累计 ACK 确认
                    if (ack.seq > base_seq && ack.seq <= next_seq)
                    {
                        for (uint32_t s = base_seq; s < ack.seq; ++s)
                        {
                            window[s % RING_SIZE].acked = true;
                        }
                    }

                    // 选择性 ACK (SACK) 处理
                    if (ack.sack_seq >= base_seq && ack.sack_seq < next_seq)
                    {
                        window[ack.sack_seq % RING_SIZE].acked = true;

                        int retransmit_limit = 2;
                        auto now = std::chrono::steady_clock::now();
                        for (uint32_t s = base_seq; s < ack.sack_seq && retransmit_limit > 0; ++s)
                        {
                            auto &pkt = window[s % RING_SIZE];
                            if (!pkt.acked)
                            {
                                auto elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(now - pkt.last_sent).count();

                                if (elapsed_ms > 100)
                                {
                                    send_data_packet(sockfd, server_addr, pkt);
                                    pkt.last_sent = now;
                                    data_packet_resent++;
                                    retransmit_limit--;
                                }
                            }
                        }
                    }
                }
            }
        }

        // 推动队头，滑动窗口（替代 pop_front）
        while (base_seq < next_seq && window[base_seq % RING_SIZE].acked)
        {
            base_seq++;
        }

        // RTO 超时检测遍历
        if (base_seq < next_seq)
        {
            auto now = std::chrono::steady_clock::now();
            for (uint32_t s = base_seq; s < next_seq; ++s)
            {
                auto &pkt = window[s % RING_SIZE];
                if (pkt.acked)
                    continue;

                auto elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                                      now - pkt.last_sent)
                                      .count();

                if (elapsed_ms > timeout_ms)
                {
                    send_data_packet(sockfd, server_addr, pkt);
                    pkt.last_sent = now;
                    data_packet_resent++;
                    timeout_count++;
                }
                else if (elapsed_ms < timeout_ms / 2)
                {
                    break;
                }
            }
        }
    }

    if (!send_fin_wait_ack(sockfd, server_addr, next_seq, fin_sent))
    {
        close(sockfd);
        return 1;
    }

    auto end_time = std::chrono::steady_clock::now();
    double seconds = std::chrono::duration<double>(end_time - start_time).count();
    double mbps = total_sent * 8.0 / seconds / 1000000.0;

    in.close();
    close(sockfd);

    std::cout << "transfer complete\n";
    std::cout << "total sent: " << total_sent << " bytes\n";
    std::cout << "time: " << seconds << " sec\n";
    std::cout << "rate: " << mbps << " Mbits/sec\n";

    std::cout << "data packets sent: " << data_packet_sent << "\n";
    std::cout << "data packets resent: " << data_packet_resent << "\n";
    std::cout << "acks received: " << ack_received << "\n";
    std::cout << "timeouts: " << timeout_count << "\n";
    std::cout << "FIN packets sent: " << fin_sent << "\n";

    return 0;
}