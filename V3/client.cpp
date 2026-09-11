#include <iostream>
#include <fstream>
#include <vector>
#include <cstring>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <chrono>
#include "common.h"

int main(int argc, char *argv[])
{
    if (argc < 2)
    {
        std::cout << "Usage: ./client <filepath>\n";
        return 1;
    }

    std::string filepath = argv[1];

    std::ifstream file(filepath, std::ios::binary | std::ios::ate);
    if (!file.is_open())
    {
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

    // 设置内核 4MB 缓冲区
    int buf_size = 4 * 1024 * 1024;
    setsockopt(sockfd, SOL_SOCKET, SO_SNDBUF, &buf_size, sizeof(buf_size));
    setsockopt(sockfd, SOL_SOCKET, SO_RCVBUF, &buf_size, sizeof(buf_size));

    // 3. 构造并发送 START 包
    PacketHeader start_hdr{};
    start_hdr.type = PKT_START;
    start_hdr.seq = 0;
    start_hdr.total_packets = total_packets;
    start_hdr.file_size = file_size;
    start_hdr.payload_len = 0;

    // time record
    auto send_start = std::chrono::steady_clock::now();
    //---

    // 连续发送 3 次 START 包防止丢包
    for (int i = 0; i < 5; ++i)
    {
        sendto(sockfd, &start_hdr, sizeof(start_hdr), 0,
               (sockaddr *)&server_addr, sizeof(server_addr));
    }

    std::cout << "[Client] START packet sent to " << SERVER_IP << ":" << SERVER_PORT << "\n";

    // 4. 按顺序发送 DATA 包 (Step 2 核心)
    std::vector<char> packet_buf(sizeof(PacketHeader) + PAYLOAD_SIZE);

    for (uint32_t seq = 0; seq < total_packets; ++seq)
    {
        // 填header
        PacketHeader *hdr = reinterpret_cast<PacketHeader *>(packet_buf.data());
        hdr->type = PKT_DATA;
        hdr->seq = seq;
        hdr->total_packets = total_packets;
        hdr->file_size = file_size;

        uint64_t offset = (uint64_t)seq * PAYLOAD_SIZE;
        uint16_t current_len = std::min((uint64_t)PAYLOAD_SIZE, file_size - offset);
        hdr->payload_len = current_len;

        // 将文件对应位置的数据拷贝到数据包负载区
        memcpy(packet_buf.data() + sizeof(PacketHeader), file_buffer.data() + offset, current_len);

        sendto(sockfd, packet_buf.data(), sizeof(PacketHeader) + current_len, 0,
               (sockaddr *)&server_addr, sizeof(server_addr));

        // 简单的控速以防本地 Socket 缓冲区瞬间塞满丢包
        if (seq % 10 == 0)
            usleep(800);
    }

    std::cout << "[Client] All DATA packets sent.\n";

    PacketHeader fin_hdr{PKT_FIN, 0, total_packets, file_size, 0};
    for (int i = 0; i < 5; ++i)
    {
        sendto(sockfd, &fin_hdr, sizeof(fin_hdr), 0, (sockaddr *)&server_addr, sizeof(server_addr));
    }

    std::cout << "[Client] PKT_FIN sent. Waiting for Server FIN confirmation...\n";

    PacketHeader ack_hdr{};
    while (true)
    {
        ssize_t bytes = recvfrom(sockfd, &ack_hdr, sizeof(ack_hdr), 0, nullptr, nullptr);
        if (bytes >= (ssize_t)sizeof(PacketHeader) && ack_hdr.type == PKT_FIN)
        {
            std::cout << "[Client] Server confirmed PKT_FIN. Transfer complete!\n";
            break;
        }
    }

    auto send_end = std::chrono::steady_clock::now();

    // Print statistics
    double send_sec = std::chrono::duration<double>(send_end - send_start).count();
    double send_rate_mbps = file_size * 8.0 / send_sec / 1000000.0;

    std::cout << "initial send time: " << send_sec << " sec\n";
    std::cout << "initial send rate: " << send_rate_mbps << " Mbits/sec\n";

    close(sockfd);
    return 0;
}