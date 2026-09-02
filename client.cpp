#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cstdlib>
#include <cstring>
#include <iostream>
#include <string>

int main(int argc, char *argv[]) {
    if (argc != 4) {
        std::cerr << "usage: " << argv[0] << " <server_ip> <server_port> <message>\n";
        return 1;
    }

    const char *server_ip = argv[1];
    int server_port = std::atoi(argv[2]);
    std::string message = argv[3];

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

    ssize_t sent = sendto(sockfd, message.c_str(), message.size(), 0,
                          reinterpret_cast<sockaddr *>(&server_addr),
                          sizeof(server_addr));
    if (sent < 0) {
        perror("sendto");
        close(sockfd);
        return 1;
    }

    std::cout << "sent " << sent << " bytes to "
              << server_ip << ":" << server_port << "\n";

    close(sockfd);
    return 0;
}
