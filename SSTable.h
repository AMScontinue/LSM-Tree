#pragma once

#include <cstdint>
#include <vector>
#include "SkipList.h"

struct HEADER {
    uint64_t time_stamp;
    uint64_t pair_num;
    uint64_t min_key;
    uint64_t max_key;
};

struct INDEX {
    uint64_t key;
    uint32_t offset; 
};

struct key_value {
    uint64_t key;
    std::string val;
    key_value(uint64_t k, std::string v) : key(k), val(v) {}
};

class SSTable {
  private:
    HEADER header;
    bool bloom_filter[10240];
    INDEX *index;
    std::string filepath;
    uint32_t total_size;

    void set_bf(uint64_t key); 

  public:
    SSTable(std::vector<key_value> &nodes, const uint64_t t_stamp, uint64_t &filenum, const std::string &dirname, const uint32_t level);
    SSTable(MemTable &memt, uint64_t &t_stamp, uint64_t &filenum, const std::string &dirname, uint64_t level = 0);
    SSTable(std::string fp);
    ~SSTable();
    bool exist(uint64_t key);
    std::string get(uint64_t key);
    uint64_t get_timestamp();
    uint64_t get_minkey();
    uint64_t get_maxkey();
    uint64_t get_pairnum();
    uint64_t get_index_key(uint64_t i);
    void set_timestamp(uint64_t ts);
    void rmfile();
    void clear();
};
