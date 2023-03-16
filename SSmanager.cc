#include "SSmanager.h"
#include "SSTable.h"
#include "utils.h"
#include <cmath>
#include <queue>
#include <algorithm>
#include <iostream>
#include <fstream>
#include <regex>

SSTable_handler::SSTable_handler() {
    t_stamp = 1;
    filenum = 0;
    std::vector<SSTable*> first_ele;
    sst_handler_list.push_back(first_ele);
}

SSTable_handler::~SSTable_handler() {
    for (std::vector<std::vector<SSTable*>>::iterator it = sst_handler_list.begin(); it != sst_handler_list.end(); it++) {
        for (std::vector<SSTable*>::iterator sit = (*it).begin(); sit != (*it).end(); sit++)
            delete *sit;
    }
}

bool SSTable_handler::exist(uint64_t key) {
    for (std::vector<std::vector<SSTable*>>::iterator it = sst_handler_list.begin(); it != sst_handler_list.end(); it++) {
        for (std::vector<SSTable*>::iterator sit = (*it).begin(); sit != (*it).end(); sit++) {
            if ((**sit).exist(key)) 
                return true;
        }
    }
    return false;
}

std::string SSTable_handler::get(uint64_t key) {
    std::vector<SSTable*> tmp;  
    for (std::vector<std::vector<SSTable*>>::iterator it = sst_handler_list.begin(); it != sst_handler_list.end(); it++) {
        for (std::vector<SSTable*>::iterator sit = (*it).begin(); sit != (*it).end(); sit++) {
            if ((**sit).exist(key)) 
                tmp.push_back(*sit);
        }
    }
    if (tmp.empty())
        return "";
    
    SSTable* newest = *(tmp.begin());
    for (std::vector<SSTable*>::iterator i = tmp.begin(); i != tmp.end(); i++)
        if ((*i)->get_timestamp() > newest->get_timestamp())
            newest = (*i);
    return newest->get(key);
}

void SSTable_handler::to_sst(MemTable &memt) {
    if (memt.empty())
        return;
    SSTable *tmp = new SSTable(memt, t_stamp, filenum, dir_name);
    sst_handler_list[0].push_back(tmp);

    if (sst_handler_list[0].size() > 2)
        compaction(0);
}

static inline uint64_t get_fn(std::string filename) {
    std::regex pattern("(\\d+).sst");
    std::smatch result;
    std::regex_match(filename, result, pattern);
    return std::stoi(result.str(1));
}
void SSTable_handler::read_files() {
    std::vector<std::string> dirs;
    utils::scanDir(dir_name, dirs);
    if (dirs.empty())
        return;
    
    uint64_t cur_ts = 0;
    uint64_t cur_fn = 0;
    uint32_t level = 0;
    for (std::vector<std::string>::iterator it = dirs.begin(); it != dirs.end(); it++) {
        std::vector<std::string> files;
        utils::scanDir(dir_name + "/" + *it, files);
        std::vector<SSTable*> tmp;
        sst_handler_list.push_back(tmp);
        for (std::vector<std::string>::iterator fit = files.begin(); fit != files.end(); fit++) {
            cur_fn = get_fn(*fit);
            SSTable *sst = new SSTable(dir_name + "/" + *it + "/" + *fit);
            cur_ts = sst->get_timestamp();
            t_stamp = t_stamp > cur_ts ? t_stamp : cur_ts;
            filenum = filenum > cur_fn ? filenum : cur_fn;
            sst_handler_list[level].push_back(sst);
        }
        level++;
    }
    t_stamp++;
    filenum++;
}

void SSTable_handler::set_dirname(const std::string &dir) {
    dir_name = dir;
    if (utils::dirExists(dir_name))
        read_files();
    else
        utils::mkdir((char*)(dir_name.data()));
}

void SSTable_handler::clear() {
    sst_handler_list.clear();
    t_stamp = 0;

    if (utils::dirExists(dir_name)) {
        std::vector<std::string> dirs;
        utils::scanDir(dir_name, dirs);
        for (std::vector<std::string>::iterator it = dirs.begin(); it != dirs.end(); it++) {
            std::vector<std::string> files;
            utils::scanDir(dir_name + "/" + *it, files);
            for (std::vector<std::string>::iterator f = files.begin(); f != files.end(); f++)
                utils::rmfile((char*)((dir_name + "/" + *it + "/" + *f).data()));
            utils::rmdir((char*)((dir_name + "/" + *it).data()));
        }
        utils::rmdir((char*)(dir_name.data()));
    }

    std::vector<SSTable*> first_ele;
    sst_handler_list.push_back(first_ele);
}

void SSTable_handler::compaction(uint32_t level) {
    std::vector<uint64_t> index_list;
    if (level == 0) { 
        index_list.push_back(0);
        index_list.push_back(1);
        index_list.push_back(2);
    } else { 
        std::priority_queue<ts_min_index> q;
        uint64_t i = 0;
        for (std::vector<SSTable*>::iterator it = sst_handler_list[level].begin(); it != sst_handler_list[level].end(); it++, i++)
            q.push(ts_min_index((*it)->get_timestamp(), (*it)->get_minkey(), i));
        uint32_t sst_num = sst_handler_list[level].size() - std::pow(2, level + 1);
        ts_min_index tmp = q.top();
        while (sst_num != 0) {
            tmp = q.top();
            index_list.push_back(tmp.index);
            q.pop();
            sst_num--;
        }
        while (!q.empty() && tmp.ts == q.top().ts) {
            tmp = q.top();
            index_list.push_back(tmp.index);
            q.pop();
        }
    }

    uint64_t minkey = sst_handler_list[level][*(index_list.begin())]->get_minkey();
    uint64_t maxkey = sst_handler_list[level][*(index_list.begin())]->get_maxkey();
    uint64_t maxt = sst_handler_list[level][*(index_list.begin())]->get_timestamp();
    for (std::vector<uint64_t>::iterator it = index_list.begin(); it != index_list.end(); it++) {
        minkey = minkey < sst_handler_list[level][(*it)]->get_minkey() ? minkey : sst_handler_list[level][(*it)]->get_minkey();
        maxkey = maxkey > sst_handler_list[level][(*it)]->get_maxkey() ? maxkey : sst_handler_list[level][(*it)]->get_maxkey();
        maxt = maxt > sst_handler_list[level][(*it)]->get_timestamp() ? maxt : sst_handler_list[level][(*it)]->get_timestamp();
    }

    uint32_t next_level = level + 1;
    std::priority_queue<key_ts_index> nodeq;
    if (sst_handler_list.size() > next_level) { 
        std::vector<uint64_t> next_index_list;
        uint64_t i = 0;
        for (std::vector<SSTable*>::iterator it = sst_handler_list[next_level].begin(); it != sst_handler_list[next_level].end(); it++, i++)
            if (((*it)->get_minkey() >= minkey && (*it)->get_minkey() <= maxkey) && ((*it)->get_maxkey() >= minkey && (*it)->get_maxkey() <= maxkey))
                next_index_list.push_back(i);

        for (std::vector<uint64_t>::iterator it = index_list.begin(); it != index_list.end(); it++)
            for (uint64_t i = 0; i < sst_handler_list[level][*it]->get_pairnum(); i++)
                nodeq.push(key_ts_index(sst_handler_list[level][*it]->get_index_key(i), sst_handler_list[level][*it]->get_timestamp(), level, *it));
        for (std::vector<uint64_t>::iterator it = next_index_list.begin(); it != next_index_list.end(); it++)
            for (uint64_t i = 0; i < sst_handler_list[next_level][*it]->get_pairnum(); i++) {
                uint64_t tmp_ts = sst_handler_list[next_level][*it]->get_timestamp();
                nodeq.push(key_ts_index(sst_handler_list[next_level][*it]->get_index_key(i), tmp_ts, next_level, *it));
                maxt = maxt > tmp_ts ? maxt : tmp_ts;
            }

        compact_kernel(nodeq, next_level, maxt, next_level == (sst_handler_list.size() - 1));

        std::sort(index_list.begin(), index_list.end());
        uint64_t k = 0;
        for (std::vector<uint64_t>::iterator it = index_list.begin(); it != index_list.end(); it++) {
            sst_handler_list[level][*it - k]->rmfile();
            delete sst_handler_list[level][*it - k];
            sst_handler_list[level].erase(sst_handler_list[level].begin() + *it - k);
            k++;
        }
        std::sort(next_index_list.begin(), next_index_list.end());
        k = 0;
        for (std::vector<uint64_t>::iterator it = next_index_list.begin(); it != next_index_list.end(); it++) {
            sst_handler_list[next_level][*it - k]->rmfile();
            delete sst_handler_list[next_level][*it - k];
            sst_handler_list[next_level].erase(sst_handler_list[next_level].begin() + *it - k);
            k++;
        }
    } else { 
        for (std::vector<uint64_t>::iterator it = index_list.begin(); it != index_list.end(); it++)
            for (uint64_t i = 0; i < sst_handler_list[level][*it]->get_pairnum(); i++)
                nodeq.push(key_ts_index(sst_handler_list[level][*it]->get_index_key(i), sst_handler_list[level][*it]->get_timestamp(), level, *it));
        std::vector<SSTable*> newlevel;
        sst_handler_list.push_back(newlevel); 

        compact_kernel(nodeq, level + 1, maxt, true);

        std::sort(index_list.begin(), index_list.end());
        uint64_t k = 0;
        for (std::vector<uint64_t>::iterator it = index_list.begin(); it != index_list.end(); it++) {
            sst_handler_list[level][*it - k]->rmfile();
            delete sst_handler_list[level][*it - k];
            sst_handler_list[level].erase(sst_handler_list[level].begin() + *it - k);
            k++;
        }
    }

    if (check_compact(level + 1))
        compaction(level + 1);
}

bool SSTable_handler::check_compact(uint32_t level) {
    uint64_t max = std::pow(2, level + 1);
    if (sst_handler_list[level].size() > max)
        return true;
    return false;
}

void SSTable_handler::compact_kernel(std::priority_queue<key_ts_index> &nodeq, uint32_t level, uint64_t ts, bool lastlevel) {
    key_ts_index cur, last = nodeq.top();
    std::vector<key_value> newsst;
    size_t mem_size = 0;
    while (!nodeq.empty()) {
        cur = nodeq.top();
        if (cur.key != last.key || cur.timestamp >= last.timestamp) { 
            std::string value = sst_handler_list[cur.level][cur.index]->get(cur.key);
            if (!(lastlevel && value == "~DELETED~")) { 
                if (mem_size + sizeof(uint64_t) + sizeof(uint32_t) + value.length() > MAXSIZE) {
                    SSTable *tmp = new SSTable(newsst, ts, filenum, dir_name, level);
                    sst_handler_list[level].push_back(tmp);
                    mem_size = 0;
                    newsst.clear();
                    continue; 
                } else {
                    newsst.push_back(key_value(cur.key, value));
                    mem_size += sizeof(uint64_t) + sizeof(uint32_t) + value.length();
                }
            }
        }
        
        last = cur;
        nodeq.pop();
    }

    if (!newsst.empty()) {
        SSTable *tmp = new SSTable(newsst, ts, filenum, dir_name, level);
        sst_handler_list[level].push_back(tmp);
    }
}
