#pragma once
#include "lock.hpp"
#include <unordered_map>
#include <unordered_set>

/**
The Word class represents a data element within the shared memory region.
It contains the actual data value and a lock for managing concurrent access.
**/
class Word {
  public:
    uint64_t word;
    Lock lock;
    Word() : word(0), lock(0) {}
    Word(Word const&) = delete;
    Word& operator=(Word const&) = delete;
};

/**
 * The Transaction class represents a transaction within the STM system.
 * It keeps track of the read and write sets of data and the read version.
 * Transactions can be either read-only or read-write.
 **/
class Transaction {
  public:
    int read_version;
    bool is_read_only = false;
    unordered_set<Lock *> read_set;
    unordered_map<uint64_t, pair<uint64_t, Lock *>> write_map;
    Transaction(int read_version, bool is_read_only)
        : read_version(read_version), is_read_only(is_read_only) {}
};
