#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cstdlib>
#include <cstring>
#include <fstream>
#include <iostream>

const int BUFFER_SIZE = 1400;

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

    char buffer[BUFFER_SIZE];

    while (in) {
        in.read(buffer, sizeof(buffer));
        std::streamsize bytes_read = in.gcount();

        if (bytes_read > 0) {
            ssize_t sent = sendto(sockfd, buffer, bytes_read, 0,
                                  reinterpret_cast<sockaddr *>(&server_addr),
                                  sizeof(server_addr));

            if (sent < 0) {
                perror("sendto");
                break;
            }

            std::cout << "sent " << sent << " bytes\n";
        }
    }

    sendto(sockfd, nullptr, 0, 0,
           reinterpret_cast<sockaddr *>(&server_addr),
           sizeof(server_addr));

    std::cout << "sent FIN\n";

    in.close();
    close(sockfd);

    return 0;
}