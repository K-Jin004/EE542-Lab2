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

    int buf_size = 4 * 1024 * 1024;
    setsockopt(sockfd, SOL_SOCKET, SO_RCVBUF, &buf_size, sizeof(buf_size));
    setsockopt(sockfd, SOL_SOCKET, SO_SNDBUF, &buf_size, sizeof(buf_size));

    std::cout << "[Server] Waiting for START packet on " << SERVER_IP << ":" << SERVER_PORT << "...\n";

    std::vector<char> recv_buf(sizeof(PacketHeader) + PAYLOAD_SIZE);
    std::vector<char> file_buffer; // 接收文件的 DRAM 缓冲区
    
    uint32_t total_packets = 0;
    uint64_t file_size = 0;
    uint32_t received_packets = 0;
    bool initialized = false;

    sockaddr_in client_addr{};
    socklen_t addr_len = sizeof(client_addr);

    while (true) {
        ssize_t bytes = recvfrom(sockfd, recv_buf.data(), sizeof(recv_buf), 0, (sockaddr*)&client_addr, &addr_len);

        if (bytes < (ssize_t)sizeof(PacketHeader)) continue;

        PacketHeader* hdr = reinterpret_cast<PacketHeader*>(recv_buf.data());

        if (hdr->type == PKT_START && !initialized) {

            total_packets = hdr->total_packets;
            file_size = hdr->file_size;
            file_buffer.resize(file_size);
            initialized = true;

            
            
            std::cout << "[Server] START received. Allocated " << file_size << " bytes in DRAM.\n";
            std::cout << "[Server] Total Packets: " << hdr->total_packets << "\n";
            std::cout << "======================================\n";
        } else if (hdr->type == PKT_DATA && initialized) {
            uint32_t seq = hdr->seq;
            uint64_t offset = (uint64_t)seq * PAYLOAD_SIZE;

            // 将负载数据直接 memcpy 到 DRAM 缓冲区的正确偏移处
            memcpy(file_buffer.data() + offset, 
                   recv_buf.data() + sizeof(PacketHeader), 
                   hdr->payload_len);

            received_packets++;

            if (received_packets % 10000 == 0 || received_packets == total_packets) {
                std::cout << "[Server] Progress: " << received_packets << "/" << total_packets << " packets.\n";
            }

            if (received_packets == total_packets) {
                std::cout << "[Server] All packets received in DRAM!\n";
                break;
            }
        }
    }

    close(sockfd);
    return 0;
}