#pragma once
#include <cstdint>
#include <cstddef>

constexpr size_t NUM_RX_THREADS = 4;
constexpr uint16_t SERVER_MAIN_PORT = 9000; // 控制/主端口 (START, FIN, NACK)
constexpr uint16_t SERVER_BASE_PORT = 9001; // 接收数据工作端口基准 (8081, 8082, 8083, 8084)
constexpr size_t PAYLOAD_SIZE = 1400;

// 硬编码 VM 的固定 IP 地址
constexpr const char* SERVER_IP = "192.168.10.100";
constexpr const char* CLIENT_IP = "192.168.20.100";

enum PacketType : uint8_t {
    PKT_START = 1,
    PKT_DATA  = 2,
    PKT_NACK  = 3,
    PKT_FIN   = 4
};

#pragma pack(push, 1)
struct PacketHeader {
    uint8_t  type;          // 包类型
    uint32_t seq;           // 包序号
    uint32_t total_packets; // 总包数
    uint64_t file_size;     // 文件总大小 (字节)
    uint16_t payload_len;   // 负载长度
    uint32_t round;         // 当前轮次
};
#pragma pack(pop)