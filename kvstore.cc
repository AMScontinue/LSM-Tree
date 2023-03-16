#include "kvstore.h"
#include <string>

KVStore::KVStore(const std::string &dir): KVStoreAPI(dir) {
    sst_h.set_dirname(dir);
}

KVStore::~KVStore() {
    sst_h.to_sst(memt);
    memt.clear();
}

/**
 * Insert/Update the key-value pair.
 * No return values for simplicity.
 */
void KVStore::put(uint64_t key, const std::string &s) {
    if (!memt.put(key, s)) {
        sst_h.to_sst(memt); 
        memt.clear(); 
        memt.put(key, s); 
    }
}
/**
 * Returns the (string) value of the given key.
 * An empty string indicates not found.
 */
std::string KVStore::get(uint64_t key) {
    std::string s = memt.get(key);
    if (s == "") 
        s = sst_h.get(key); 

    if (s == "~DELETED~")
        return "";
    else
        return s;
}
/**
 * Delete the given key-value pair if it exists.
 * Returns false iff the key is not found.
 */
bool KVStore::del(uint64_t key) {
    std::string tmp1 = memt.get(key);
    if (tmp1 == "~DELETED~") 
        return false;

    std::string tmp2 = sst_h.get(key);
    if (tmp1 != "" || (tmp2 != "" && tmp2 != "~DELETED~")) { 
        memt.remove(key); 
        std::string tmp = "~DELETED~";
        put(key, tmp);
        return true; 
    }
    
    memt.remove(key); 
    std::string tmp = "~DELETED~";
    put(key, tmp);
    return false;
}

/**
 * This resets the kvstore. All key-value pairs should be removed,
 * including memtable and all sstables files.
 */
void KVStore::reset() {
    memt.clear();
    sst_h.clear();
}

void KVStore::scan(uint64_t key1, uint64_t key2, std::list<std::pair<uint64_t, std::string> > &list)
{
    int i=1;
    i++;

}
