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

    char buffer[BUFFER_SIZE];

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

        if (n == 0) {
            std::cout << "received FIN, transfer done\n";
            break;
        }

        out.write(buffer, n);
        std::cout << "received " << n << " bytes\n";
    }

    out.close();
    std::cout << "file saved\n";

    close(sockfd);

    return 0;


}
