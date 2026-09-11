#include <iostream>
#include <fstream>
#include <vector>
#include <cstring>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include "common.h"

int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cout << "Usage: ./client <filepath>\n";
        return 1;

    }

    std::string filepath = argv[1];

    std::ifstream file(filepath, std::ios::binary | std::ios::ate);
    if (!file.is_open()) {
        std::cerr << "Failed to open file: " << filepath << "\n";
        return 1;
    }

    uint64_t file_size = file.tellg();
    file.seekg(0, std::ios::beg);
    std::vector<char> file_buffer(file_size);
    file.read(file_buffer.data(), file_size);
    file.close();

    uint32_t total_packets = (file_size + PAYLOAD_SIZE - 1) / PAYLOAD_SIZE;
    std::cout << "[Client] File loaded. Size: " << file_size << " bytes, Total Packets: " << total_packets << "\n";

    // 2. 创建 Socket 并使用头文件里的 SERVER_IP
    int sockfd = socket(AF_INET, SOCK_DGRAM, 0);
    sockaddr_in server_addr{};
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(SERVER_PORT);
    inet_pton(AF_INET, SERVER_IP, &server_addr.sin_addr);

    // 3. 构造并发送 START 包
    PacketHeader start_hdr{};
    start_hdr.type = PKT_START;
    start_hdr.seq = 0;
    start_hdr.total_packets = total_packets;
    start_hdr.file_size = file_size;
    start_hdr.payload_len = 0;

    // 连续发送 3 次 START 包防止丢包
    for (int i = 0; i < 5; ++i) {
        sendto(sockfd, &start_hdr, sizeof(start_hdr), 0, 
               (sockaddr*)&server_addr, sizeof(server_addr));
    }


    std::cout << "[Client] START packet sent to " << SERVER_IP << ":" << SERVER_PORT << "\n";

    close(sockfd);
    return 0;
}