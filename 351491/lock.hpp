#pragma once
#include <atomic>

using namespace std;

// It uses an atomic integer to maintain a version and lock state.
// A lock is acquired by setting the least significant bit (LSB) in the version
class Lock {
  private:
    atomic_int lock;

  public:
    Lock(int a) : lock(a) {}

    Lock(const Lock &other) : lock(other.lock.load()) {}

    int get_version() {
        int version = lock.load();
        if ((version & 1)) {
            return -1; // lock taken
        }
        return version >> 1;
    }

    bool acquire_lock(int read_version) {
        int version = lock.load();
        if ((version & 1) > 0 || (version >> 1) > read_version) {
            return false;
        }
        return atomic_compare_exchange_strong(&lock, &version, version | 1);
    }

    void update_version(int version) { lock.store(version << 1); }

    void unlock() { lock.fetch_sub(1); }
};
