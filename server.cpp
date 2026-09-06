#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <chrono>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <iostream>
#include <map>
#include <vector>

const int CHUNK_SIZE = 1400;
const uint64_t PRINT_INTERVAL = 10ULL * 1024 * 1024;

enum PacketType
{
    DATA = 1,
    ACK = 2,
    FIN = 3
};

struct PacketHeader
{
    uint32_t type;
    uint32_t seq;
    uint32_t length;
};

void send_ack(int sockfd, sockaddr_in &client_addr, socklen_t client_len, uint32_t seq)
{
    PacketHeader ack{};
    ack.type = ACK;
    ack.seq = seq;
    ack.length = 0;

    sendto(sockfd, &ack, sizeof(ack), 0,
           reinterpret_cast<sockaddr *>(&client_addr),
           client_len);
}

void print_progress(uint64_t total_received, uint64_t &next_print)
{
    while (total_received >= next_print)
    {
        std::cout << "received "
                  << (next_print / (1024 * 1024))
                  << " MB\n";

        next_print += PRINT_INTERVAL;
    }
}

int main(int argc, char *argv[])
{
    if (argc < 3 || argc > 4)
    {
        std::cerr << "usage: " << argv[0]
                  << " <listen_port> <output_file> [max_chunk_size]\n";
        return 1;
    }

    int port = std::atoi(argv[1]);
    const char *output_file = argv[2];

    int max_chunk_size = CHUNK_SIZE;

    if (argc >= 4)
    {
        max_chunk_size = std::atoi(argv[3]);
    }

    if (max_chunk_size <= 0)
    {
        std::cerr << "max_chunk_size must be positive\n";
        return 1;
    }

    int sockfd = socket(AF_INET, SOCK_DGRAM, 0);

    if (sockfd < 0)
    {
        perror("socket");
        return 1;
    }

    sockaddr_in server_addr{};
    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = INADDR_ANY;
    server_addr.sin_port = htons(port);

    if (bind(sockfd, reinterpret_cast<sockaddr *>(&server_addr), sizeof(server_addr)) < 0)
    {
        perror("bind");
        close(sockfd);
        return 1;
    }

    std::ofstream out(output_file, std::ios::binary);
    if (!out)
    {
        std::cerr << "cannot open output file\n";
        close(sockfd);
        return 1;
    }

    int buf_size = 4 * 1024 * 1024; // 4MB
    if (setsockopt(sockfd, SOL_SOCKET, SO_RCVBUF, &buf_size, sizeof(buf_size)) < 0)
    {
        perror("setsockopt SO_RCVBUF");
    }
    if (setsockopt(sockfd, SOL_SOCKET, SO_SNDBUF, &buf_size, sizeof(buf_size)) < 0)
    {
        perror("setsockopt SO_SNDBUF");
    }

    std::cout << "server listening on port " << port << "\n";

    std::vector<char> buffer(sizeof(PacketHeader) + max_chunk_size);

    uint32_t expected_seq = 0;
    uint64_t total_received = 0;
    uint64_t next_print = PRINT_INTERVAL;

    // ---- statistics
    uint64_t data_packet_received = 0;      // 收到 DATA 包总数，包括重复
    uint64_t data_packet_written = 0;       // 真正写入文件的 DATA 包数
    uint64_t duplicate_packet_received = 0; // seq < expected_seq
    uint64_t out_of_order_received = 0;     // seq > expected_seq
    uint64_t ack_sent = 0;
    uint64_t fin_received = 0;
    // ----

    std::map<uint32_t, std::vector<char>> pending;

    while (true)
    {
        sockaddr_in client_addr{};
        socklen_t client_len = sizeof(client_addr);

        ssize_t n = recvfrom(sockfd, buffer.data(), buffer.size(), 0,
                             reinterpret_cast<sockaddr *>(&client_addr),
                             &client_len);

        if (n < 0)
        {
            perror("recvfrom");
            continue;
        }

        PacketHeader header{};
        std::memcpy(&header, buffer.data(), sizeof(PacketHeader));

        if (header.type == DATA)
        {
            data_packet_received++;
            char *payload = buffer.data() + sizeof(PacketHeader);

            send_ack(sockfd, client_addr, client_len, header.seq);
            
            ack_sent++;

            if (header.seq < expected_seq)
            {
                duplicate_packet_received++;
                continue;
            }

            if (header.seq == expected_seq)
            {
                out.write(payload, header.length);
                total_received += header.length;
                data_packet_written++;
                print_progress(total_received, next_print);
                expected_seq++;

                while (pending.count(expected_seq))
                {
                    auto &data = pending[expected_seq];
                    out.write(data.data(), data.size());
                    total_received += data.size();
                    data_packet_written++;
                    print_progress(total_received, next_print);
                    pending.erase(expected_seq);
                    expected_seq++;
                }
            }
            else
            {
                out_of_order_received++;
                if (!pending.count(header.seq))
                {
                    pending[header.seq] =
                        std::vector<char>(payload, payload + header.length);
                }
            }
        }
        else if (header.type == FIN)
        {
            fin_received++;
            send_ack(sockfd, client_addr, client_len, header.seq);
            std::cout << "received FIN\n";

            timeval timeout{};
            timeout.tv_sec = 2;
            timeout.tv_usec = 0;
            setsockopt(sockfd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));

            while (true)
            {
                sockaddr_in repeat_client{};
                socklen_t repeat_len = sizeof(repeat_client);

                ssize_t n = recvfrom(sockfd, buffer.data(), buffer.size(), 0,
                                     reinterpret_cast<sockaddr *>(&repeat_client),
                                     &repeat_len);

                if (n < 0)
                {
                    break; // 2 seconds passed, no more duplicate FIN
                }

                if (n < static_cast<ssize_t>(sizeof(PacketHeader)))
                {
                    continue;
                }

                PacketHeader repeat_header{};
                std::memcpy(&repeat_header, buffer.data(), sizeof(PacketHeader));

                if (repeat_header.type == FIN && repeat_header.seq == header.seq)
                {
                    send_ack(sockfd, repeat_client, repeat_len, repeat_header.seq);
                    std::cout << "re-ACK duplicate FIN\n";
                }
            }

            break;
        }
    }

    out.close();
    std::cout << "saved file: " << output_file << "\n";

    close(sockfd);
    std::cout << "total received: " << total_received << " bytes\n";

    std::cout << "data packets received: " << data_packet_received << "\n";
    std::cout << "data packets written: " << data_packet_written << "\n";
    std::cout << "duplicate packets received: " << duplicate_packet_received << "\n";
    std::cout << "out-of-order packets received: " << out_of_order_received << "\n";
    std::cout << "ACK packets sent: " << ack_sent << "\n";
    std::cout << "FIN packets received: " << fin_received << "\n";
    std::cout << "pending packets left: " << pending.size() << "\n";

    return 0;
}
