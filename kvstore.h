#pragma once

#include "kvstore_api.h"
#include "SkipList.h"
#include "SSmanager.h"

class KVStore : public KVStoreAPI {
	// You can add your implementation here
private:
	MemTable memt;
	SSTable_handler sst_h;

public:
	KVStore(const std::string &dir);

	~KVStore();

	void put(uint64_t key, const std::string &s) override;

	std::string get(uint64_t key) override;

	bool del(uint64_t key) override;

	void reset() override;

        void scan(uint64_t key1, uint64_t key2, std::list<std::pair<uint64_t, std::string> > &list) override;

};
