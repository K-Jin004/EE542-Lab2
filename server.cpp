#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cstdlib>
#include <cstring>
#include <iostream>

int main(int argc, char *argv[]) {
    if (argc != 2) {
        std::cerr << "usage: " << argv[0] << " <listen_port>\n";
        return 1;
    }

    int port = std::atoi(argv[1]);

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

    std::cout << "UDP server listening on port " << port << "\n";

    char buffer[2048];
    while (true) {
        sockaddr_in client_addr{};
        socklen_t client_len = sizeof(client_addr);

        ssize_t n = recvfrom(sockfd, buffer, sizeof(buffer) - 1, 0,
                             reinterpret_cast<sockaddr *>(&client_addr),
                             &client_len);
        if (n < 0) {
            perror("recvfrom");
            continue;
        }

        buffer[n] = '\0';

        char client_ip[INET_ADDRSTRLEN];
        inet_ntop(AF_INET, &client_addr.sin_addr, client_ip, sizeof(client_ip));

        std::cout << "received from " << client_ip << ":"
                  << ntohs(client_addr.sin_port) << " -> "
                  << buffer << "\n";
    }

    close(sockfd);
    return 0;
}
