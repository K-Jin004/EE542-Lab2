#include <iostream>
#include <vector>
#include <cstring>
#include <sys/socket.h>
#include <netinet/in.h>
#include <unistd.h>
#include "common.h"

int main() {
    int sockfd = socket(AF_INET, SOCK_DGRAM, 0);
    
    sockaddr_in server_addr{};
    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = INADDR_ANY; // 监听所有本地网卡
    server_addr.sin_port = htons(SERVER_PORT);

    if (bind(sockfd, (sockaddr*)&server_addr, sizeof(server_addr)) < 0) {
        std::cerr << "Bind failed!\n";
        return 1;
    }

    std::cout << "[Server] Waiting for START packet on " << SERVER_IP << ":" << SERVER_PORT << "...\n";

    char recv_buf[sizeof(PacketHeader)];
    sockaddr_in client_addr{};
    socklen_t addr_len = sizeof(client_addr);

    while (true) {
        ssize_t bytes = recvfrom(sockfd, recv_buf, sizeof(recv_buf), 0, 
                                 (sockaddr*)&client_addr, &addr_len);
        if (bytes < (ssize_t)sizeof(PacketHeader)) continue;

        PacketHeader* hdr = reinterpret_cast<PacketHeader*>(recv_buf);

        if (hdr->type == PKT_START) {
            std::cout << "\n[Server] === START Packet Received ===" << "\n";
            std::cout << "File Size    : " << hdr->file_size << " bytes\n";
            std::cout << "Total Packets: " << hdr->total_packets << "\n";
            std::cout << "======================================\n";
            break;
        }
    }

    close(sockfd);
    return 0;
}