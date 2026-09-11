#pragma once
#include <cstdint>
#include <cstddef>

constexpr uint16_t SERVER_PORT = 8080;
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
};
#pragma pack(pop)