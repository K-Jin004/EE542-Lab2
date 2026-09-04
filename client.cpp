#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

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

struct PacketState
{
    PacketHeader header;
    std::vector<char> payload;
    bool acked;
    std::chrono::steady_clock::time_point last_sent;
};

void send_data_packet(int sockfd, sockaddr_in &server_addr, PacketState &pkt)
{
    std::vector<char> buffer(sizeof(PacketHeader) + pkt.header.length);

    std::memcpy(buffer.data(), &pkt.header, sizeof(PacketHeader));
    std::memcpy(buffer.data() + sizeof(PacketHeader),
                pkt.payload.data(),
                pkt.header.length);

    sendto(sockfd,
           buffer.data(),
           buffer.size(),
           0,
           reinterpret_cast<sockaddr *>(&server_addr),
           sizeof(server_addr));
}

bool send_fin_wait_ack(int sockfd, sockaddr_in &server_addr, uint32_t fin_seq)
{
    PacketHeader fin{};
    fin.type = FIN;
    fin.seq = fin_seq;
    fin.length = 0;

    while (true)
    {
        ssize_t sent = sendto(sockfd,
                              &fin,
                              sizeof(fin),
                              0,
                              reinterpret_cast<sockaddr *>(&server_addr),
                              sizeof(server_addr));

        if (sent < 0)
        {
            perror("sendto FIN");
            return false;
        }

        PacketHeader ack{};
        ssize_t n = recvfrom(sockfd,
                             &ack,
                             sizeof(ack),
                             0,
                             nullptr,
                             nullptr);

        if (n >= static_cast<ssize_t>(sizeof(PacketHeader)) &&
            ack.type == ACK &&
            ack.seq == fin_seq)
        {
            std::cout << "FIN acknowledged\n";
            return true;
        }

        std::cout << "timeout or wrong ACK, resend FIN\n";
    }
}

int main(int argc, char *argv[])
{
    if (argc != 4 || argc > 7)
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

    // setting time out
    timeval timeout{};
    timeout.tv_sec = timeout_ms / 1000;
    timeout.tv_usec = (timeout_ms % 1000) * 1000;

    if (setsockopt(sockfd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout)) < 0)
    {
        perror("setsockopt");
        close(sockfd);
        return 1;
    }
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

    std::deque<PacketState> window;
    uint32_t next_seq = 0;
    uint64_t total_sent = 0;
    bool file_done = false;
    uint64_t next_print = PRINT_INTERVAL;

    auto start_time = std::chrono::steady_clock::now();

    while (!file_done || !window.empty())
    {
        // send files until window full
        while (!file_done && window.size() < window_size)
        {
            std::vector<char> payload(chunk_size);

            in.read(payload.data(), chunk_size);
            std::streamsize bytes_read = in.gcount();

            if (bytes_read <= 0)
            {
                file_done = true;
                break;
            }

            payload.resize(bytes_read);

            PacketHeader header{};
            header.type = DATA;
            header.seq = next_seq;
            header.length = bytes_read;

            PacketState pkt{};
            pkt.header = header;
            pkt.payload = payload;
            pkt.acked = false;

            send_data_packet(sockfd, server_addr, pkt);
            pkt.last_sent = std::chrono::steady_clock::now();

            window.push_back(pkt);
            total_sent += bytes_read;
            if (total_sent >= next_print)
            {
                std::cout << "sent " << (total_sent / (1024 * 1024)) << " MB\n";
                next_print += PRINT_INTERVAL;
            }

            next_seq++;
        }

        PacketHeader ack{};
        ssize_t n = recvfrom(sockfd, &ack, sizeof(ack), 0, nullptr, nullptr);

        if (ack.type == ACK)
        {
            for (auto &pkt : window)
            {
                if (pkt.header.seq == ack.seq)
                {
                    pkt.acked = true;
                    break;
                }
            }
        }

        while (!window.empty() && window.front().acked)
        {
            window.pop_front();
        }

        // resend
        auto now = std::chrono::steady_clock::now();
        for (auto &pkt : window)
        {
            auto elapsed = std::chrono::duration_cast<std::chrono::microseconds>(now - pkt.last_sent).count();

            if (!pkt.acked && elapsed > timeout_ms * 1000)
            {
                send_data_packet(sockfd, server_addr, pkt);
                pkt.last_sent = now;
            }
        }
    }

    if (!send_fin_wait_ack(sockfd, server_addr, next_seq))
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

    return 0;
}