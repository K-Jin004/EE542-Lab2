#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <chrono>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <iostream>

const int CHUNK_SIZE = 1400;
const int TIMEOUT_SEC = 0;
const int TIMEOUT_USEC = 50000; //50 ms



enum PacketType {
    DATA = 1,
    ACK = 2,
    FIN = 3
};

struct PacketHeader {
    uint32_t type;
    uint32_t seq;
    uint32_t length;
};

bool wait_for_ack(int sockfd, uint32_t expected_seq) {
    PacketHeader ack{};

    ssize_t n = recvfrom(sockfd, &ack, sizeof(ack), 0, nullptr, nullptr);

    if (n < 0) {
        std::cout << "ACK timeout\n";
        return false;
    }

    return ack.type == ACK && ack.seq == expected_seq;
}

bool send_packet_wait_ack(int sockfd, sockaddr_in &server_addr, PacketHeader &header, const char *payload) {
    
    // compose packet
    char packet[sizeof(PacketHeader) + CHUNK_SIZE];
    std::memcpy(packet, &header, sizeof(PacketHeader));

    if (header.length > 0) {
        std::memcpy(packet + sizeof(PacketHeader), payload, header.length);
    }
    //
    // send and wait for ack
    size_t packet_size = sizeof(PacketHeader) + header.length;

    while (true) {
        ssize_t sent = sendto(sockfd, packet, packet_size, 0,
                              reinterpret_cast<sockaddr *>(&server_addr),
                              sizeof(server_addr));

        if (sent < 0) {
            perror("sendto");
            return false;
        }

        if (wait_for_ack(sockfd, header.seq)) {
            return true;
        }

        std::cout << "resend seq=" << header.seq << "\n";
    }
}

int main(int argc, char *argv[]) {
    if (argc != 4) {
        std::cerr << "usage: " << argv[0] << " <server_ip> <server_port> <input_file>\n";
        return 1;
    }

    const char *server_ip = argv[1];
    int server_port = std::atoi(argv[2]);
    const char *input_file = argv[3];

    int sockfd = socket(AF_INET, SOCK_DGRAM, 0);
    if (sockfd < 0) {
        perror("socket");
        return 1;
    }

    // setting time out
    timeval timeout{};
    timeout.tv_sec = TIMEOUT_SEC;
    timeout.tv_usec = TIMEOUT_USEC;

    if (setsockopt(sockfd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout)) < 0) {
        perror("setsockopt");
        close(sockfd);
        return 1;
    }
    //

    sockaddr_in server_addr{};
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(server_port);

    if (inet_pton(AF_INET, server_ip, &server_addr.sin_addr) != 1) {
        std::cerr << "invalid server ip\n";
        close(sockfd);
        return 1;
    }

    std::ifstream in(input_file, std::ios::binary);
    if (!in) {
        std::cerr << "cannot open input file\n";
        close(sockfd);
        return 1;
    }

    char payload[CHUNK_SIZE];
    uint32_t seq = 0;
    uint64_t total_sent = 0;

    auto start_time = std::chrono::steady_clock::now();

    while(true) {
        in.read(payload, CHUNK_SIZE);
        std::streamsize bytes_read = in.gcount();

        if (bytes_read <= 0) {
            break;
        }

        PacketHeader header{};
        header.type = DATA;
        header.seq = seq;
        header.length = static_cast<uint32_t>(bytes_read);

        if (!send_packet_wait_ack(sockfd, server_addr, header, payload)) {
            close(sockfd);
            return 1;
        }

        total_sent += bytes_read;
        seq++;

        if (seq % 1000 == 0) {
            std::cout << "sent packets: " << seq
                      << ", bytes: " << total_sent << "\n";
        }

    }

    PacketHeader fin{};
    fin.type = FIN;
    fin.seq = seq;
    fin.length = 0;

    if (!send_packet_wait_ack(sockfd, server_addr, fin, nullptr)) {
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