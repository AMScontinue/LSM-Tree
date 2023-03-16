#include "SSTable.h"
#include "SSmanager.h"
#include <fstream>
#include <iostream>
#include "MurmurHash3.h"
#include "utils.h"

SSTable::SSTable(std::vector<key_value> &nodes, const uint64_t t_stamp, uint64_t &filenum, const std::string &dirname, const uint32_t level) {
    filepath = dirname + "/level-" +std::to_string(level) + "/" + std::to_string(filenum++) + ".sst";
    header.time_stamp = t_stamp;
    header.pair_num = nodes.size();
    header.min_key = (*nodes.begin()).key;
    header.max_key = (*(nodes.end()-1)).key;
    uint64_t i = 0;
    uint32_t offset = sizeof(header) + sizeof(bloom_filter) + (sizeof(uint64_t) + sizeof(uint32_t)) * header.pair_num;
    index = new INDEX[header.pair_num];
    for (std::vector<key_value>::iterator it = nodes.begin(); it != nodes.end(); it++) {
        index[i].key = (*it).key;
        index[i].offset = offset;
        i++;
        offset += (*it).val.length();
        set_bf((*it).key);
    }
    total_size = offset;

    std::string path = "data/level-" + std::to_string(level);
    if (!utils::dirExists(path))
        utils::mkdir((char*)(path.data()));
    std::ofstream file(filepath, std::ios::binary);
    if (!file.is_open()) {
        std::cerr << "write: can not open " << filepath << std::endl;
        exit(-1);
    }
    file.write(reinterpret_cast<char*>(&header), sizeof(header));
    file.write(reinterpret_cast<char*>(bloom_filter), sizeof(bloom_filter));
    for (i = 0; i < header.pair_num; i++)
        file.write(reinterpret_cast<char*>(&index[i]), sizeof(uint64_t) + sizeof(uint32_t));
    for (std::vector<key_value>::iterator it = nodes.begin(); it != nodes.end(); it++)
        file.write((char*)((*it).val.data()), sizeof(char) * (*it).val.length());
    file.close();
}

SSTable::SSTable(MemTable &memt, uint64_t &t_stamp, uint64_t &filenum, const std::string &dirname, uint64_t level) {
    filepath = dirname + "/level-" + std::to_string(level) + "/" + std::to_string(filenum++) + ".sst";
    NODE *key_val = memt.key_value()->right;
    header.time_stamp = t_stamp++;
    header.pair_num = memt.size();
    header.min_key = key_val->key;
    NODE *tmp = key_val;
    uint64_t i = 0;
    uint32_t offset = sizeof(header) + sizeof(bloom_filter) + (sizeof(uint64_t) + sizeof(uint32_t)) * header.pair_num;
    index = new INDEX[header.pair_num];
    while (tmp != nullptr) { 
        header.max_key = tmp->key;
        index[i].key = tmp->key;
        index[i].offset = offset;
        i++;
        offset += tmp->val.length();
        set_bf(tmp->key);
        tmp = tmp->right;
    }
    total_size = offset;

    std::string path = "data/level-" + std::to_string(level);
    if(!utils::dirExists(path))
        utils::mkdir((char*)(path.data()));
    std::ofstream file(filepath, std::ios::binary);
    if (!file.is_open()) {
        std::cerr << "write: can not open " << filepath << std::endl;
        exit(-1);
    }
    file.write(reinterpret_cast<char*>(&header), sizeof(header));
    file.write(reinterpret_cast<char*>(bloom_filter), sizeof(bloom_filter));
    for (i = 0; i < header.pair_num; i++)
        file.write(reinterpret_cast<char*>(&index[i]), sizeof(uint64_t) + sizeof(uint32_t));
    tmp = key_val;
    while (tmp != nullptr) {
        file.write((char*)((tmp->val).data()), sizeof(char) * tmp->val.length());
        tmp = tmp->right;
    }
    file.close();
}

SSTable::SSTable(std::string fp) {
    filepath = fp;
    std::ifstream file(filepath, std::ios::binary);
    if (!file.is_open()) {
        std::cerr << "read: can not open " << filepath << std::endl;
        exit(-1);
    }

    file.read(reinterpret_cast<char*>(&header), sizeof(header));
    file.read(reinterpret_cast<char*>(bloom_filter), sizeof(bloom_filter));
    index = new INDEX[header.pair_num];
    uint64_t i;
    for (i = 0; i < header.pair_num; i++)
        file.read(reinterpret_cast<char*>(&index[i]), sizeof(uint64_t) + sizeof(uint32_t));
    file.seekg(0, std::ios::end);
    total_size = file.tellg();
    file.close();
}

SSTable::~SSTable() {
    delete [] index;
}

void SSTable::set_bf(uint64_t key) {
    unsigned int hash[4] = {0};
    MurmurHash3_x64_128(&key, sizeof(key), 1, hash);

    bloom_filter[hash[0] % 10240] = true;
    bloom_filter[hash[1] % 10240] = true;
    bloom_filter[hash[2] % 10240] = true;
    bloom_filter[hash[3] % 10240] = true;
}

bool SSTable::exist(uint64_t key) {
    unsigned int hash[4] = {0};
    MurmurHash3_x64_128(&key, sizeof(key), 1, hash);

    if (!bloom_filter[hash[0] % 10240] || !bloom_filter[hash[1] % 10240] || !bloom_filter[hash[2] % 10240] || !bloom_filter[hash[3] % 10240])
        return false;
    
    uint64_t left = 0, right = header.pair_num - 1, middle = 0;
    while (left <= right) {
        middle = left + (right - left) / 2;
        if (index[middle].key == key) {
            return true;
        }
        if (index[middle].key > key) {
            if (middle <= 0)
                break;
            right = middle - 1;
        } else
            left = middle + 1;
    }
    return false;
}

std::string SSTable::get(uint64_t key) {
    uint64_t left = 0, right = header.pair_num - 1, middle = 0;
    uint32_t offset;
    bool flag = false;
    while (left <= right) {
        middle = left + (right - left) / 2;
        if (index[middle].key == key) {
            offset = index[middle].offset;
            flag = true;
            break;
        }
        if (index[middle].key > key) {
            if (middle <= 0)
                break;
            right = middle - 1;
        } else
            left = middle + 1;
    }
    if (!flag)
        return "";
    
    std::string value;
    std::ifstream file(filepath, std::ios::binary);
    if (!file.is_open()) {
        std::cerr << "read: can not open " << filepath << std::endl;
        exit(-1);
    }
    file.seekg(offset);
    uint32_t num_char;
    if (middle == header.pair_num - 1) 
        num_char = total_size - offset;
    else 
        num_char = index[middle + 1].offset - offset;
    
    char tmp;
    for (uint32_t i = 0; i < num_char; i++) {
        file.read(&tmp, sizeof(tmp));
        value += tmp;
    }
    file.close();
    
    return value;
}

uint64_t SSTable::get_timestamp() {
    return header.time_stamp;
}

uint64_t SSTable::get_minkey() {
    return header.min_key;
}

uint64_t SSTable::get_maxkey() {
    return header.max_key;
}

uint64_t SSTable::get_pairnum() {
    return header.pair_num;
}

uint64_t SSTable::get_index_key(uint64_t i) {
    return index[i].key;
}

void SSTable::rmfile() {
    utils::rmfile((char*)(filepath.data()));
}
