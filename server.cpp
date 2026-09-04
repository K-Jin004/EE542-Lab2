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

void send_ack(int sockfd, sockaddr_in &client_addr, socklen_t client_len, uint32_t seq) {
    PacketHeader ack{};
    ack.type = ACK;
    ack.seq = seq;
    ack.length = 0;

    sendto(sockfd, &ack, sizeof(ack), 0,
           reinterpret_cast<sockaddr *>(&client_addr),
           client_len);
}


int main(int argc, char *argv[]) {
    if (argc != 3) {
        std::cerr << "usage: " << argv[0] << " <listen_port> <output_file>";
        return 1;
    }

    int port = std::atoi(argv[1]);
    const char *output_file = argv[2];

    int sockfd = socket(AF_INET, SOCK_DGRAM, 0);

    if (sockfd < 0) {
        perror("socket");
        return 1;
    }

    sockaddr_in server_addr{};
    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = INADDR_ANY;
    server_addr.sin_port = htons(port);

    if (bind(sockfd, reinterpret_cast<sockaddr *>(&server_addr), sizeof(server_addr)) < 0) {
        perror("bind");
        close(sockfd);
        return 1;
    }

    std::ofstream out(output_file, std::ios::binary);
    if (!out) {
        std::cerr << "cannot open output file\n";
        close(sockfd);
        return 1;
    }

    std::cout << "server listening on port " << port << "\n";

    char buffer[sizeof(PacketHeader) + CHUNK_SIZE];

    uint32_t expected_seq = 0;
    uint64_t total_received = 0;



    while (true) {
        sockaddr_in client_addr{};
        socklen_t client_len = sizeof(client_addr);

        ssize_t n = recvfrom(sockfd, buffer, sizeof(buffer), 0,
                             reinterpret_cast<sockaddr *>(&client_addr),
                             &client_len);

        if (n < 0) {
            perror("recvfrom");
            continue;
        }
        
        PacketHeader header{};
        std::memcpy(&header, buffer, sizeof(PacketHeader));

        if (header.type == DATA) {
            char *payload = buffer + sizeof(PacketHeader);

            if (header.seq == expected_seq) {
                out.write(payload, header.length);
                total_received += header.length;

                send_ack(sockfd, client_addr, client_len, header.seq);
                expected_seq++;

                if (expected_seq % 1000 == 0) {
                    std::cout << "received packets: " << expected_seq
                              << ", bytes: " << total_received << "\n";
                }
            } else if (header.seq < expected_seq) {
                //duplicate packet
                send_ack(sockfd, client_addr, client_len, header.seq);
            } else {
                // incase random behavior
                std::cout << "out of order packet seq=" << header.seq
                          << ", expected=" << expected_seq << "\n";
            }
        } else if (header.type == FIN) {
            send_ack(sockfd, client_addr, client_len, header.seq);
            std::cout << "received FIN\n";
            break;
        }

    }

    out.close();
    std::cout << "saved file: " << output_file << "\n";

    close(sockfd);
    std::cout << "total received: " << total_received << " bytes\n";

    return 0;


}
