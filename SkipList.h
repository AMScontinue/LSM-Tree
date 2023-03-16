#pragma once

#include <cstdint>
#include <string>
#define MAXSIZE 2086880

struct NODE {
    NODE *right, *down;
    uint64_t key;
    std::string val;
    NODE(NODE *right, NODE *down, uint64_t key, std::string val)
        : right(right), down(down), key(key), val(val) {}
    NODE(): right(nullptr), down(nullptr) {}
};

class MemTable {
  private:
    NODE *head;
    size_t memt_size;
    uint64_t node_num;

  public:
    MemTable();
    ~MemTable();
    size_t size();
    std::string get(const uint64_t &key);
    bool put(const uint64_t &key, const std::string &val);
    bool remove(const uint64_t &key);
    void clear();
    NODE* key_value();
    bool empty();
};

