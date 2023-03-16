#pragma once

#include <vector>
#include <queue>
#include "SSTable.h"
struct ts_min_index {
    uint64_t ts; 
    uint64_t min; 
    uint64_t index; 
    ts_min_index(uint64_t ts, uint64_t min, uint64_t index) : ts(ts), min(min), index(index) {}
    bool operator < (const ts_min_index &b) const {
        if (ts == b.ts)
            return min > b.min;
        return ts > b.ts;
    }
};

struct key_ts_index {
    uint64_t key; 
    uint64_t timestamp; 
    uint32_t level; 
    uint64_t index; 
    key_ts_index() {}
    key_ts_index(uint64_t key, uint64_t timestamp, uint32_t level, uint64_t index) : key(key), timestamp(timestamp), level(level), index(index) {}
    bool operator < (const key_ts_index &b) const {
        if (key == b.key)
            return timestamp < b.timestamp;
        return key > b.key;
    }
};

class SSTable_handler {
   private:
    std::vector<std::vector<SSTable*>> sst_handler_list;
    uint64_t t_stamp;
    uint64_t filenum;
    std::string dir_name;

    bool check_compact(uint32_t level);
    void compact_kernel(std::priority_queue<key_ts_index> &nodeq, uint32_t level, uint64_t ts, bool lastlevel = false);
    void read_files();

   public:
    SSTable_handler();
    ~SSTable_handler();
    bool exist(uint64_t key);
    std::string get(uint64_t key);
    void to_sst(MemTable &memt);
    void set_dirname(const std::string &dirname);
    void clear();
    void compaction(uint32_t level);
};
