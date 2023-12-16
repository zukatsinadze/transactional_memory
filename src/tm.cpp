/**
 * @file   tm.cpp
 * @author Zurab Tsinadze
 *
 * @section DESCRIPTION
 *
 * Implementation of transaction manager based on TL2 paper.
 *
 **/

#include "tm.hpp"
#include "lock.hpp"
#include "macros.hpp"
#include "region.hpp"
#include "transaction.hpp"
#include <unordered_set>

/** Create (i.e. allocate + init) a new shared memory region, with one first
 *non-free-able allocated segment of the requested size and alignment.
 * @param size  Size of the first shared segment of memory to allocate (in
 *bytes), must be a positive multiple of the alignment
 * @param align Alignment (in bytes, must be a power of 2) that the shared
 *memory region must support
 * @return Opaque shared memory region handle, 'invalid_shared' on failure
 **/
shared_t tm_create(size_t size, size_t align) noexcept {
    Region *region = new Region(size, align);
    if (unlikely(!region))
        return invalid_shared;
    return region;
}

/** Destroy (i.e. clean-up + free) a given shared memory region.
 * @param shared Shared memory region to destroy, with no running transaction
 **/
void tm_destroy(shared_t shared) noexcept {
    delete reinterpret_cast<Region *>(shared);
}

/** [thread-safe] Return the start address of the first allocated segment in the
 *shared memory region.
 * @param shared Shared memory region to query
 * @return Start address of the first allocated segment
 **/
void *tm_start(shared_t shared) noexcept {
    return reinterpret_cast<Region *>(shared)->get_first_segment();
}

/** [thread-safe] Return the size (in bytes) of the first allocated segment of
 *the shared memory region.
 * @param shared Shared memory region to query
 * @return First allocated segment size
 **/
size_t tm_size(shared_t shared) noexcept {
    return reinterpret_cast<Region *>(shared)->get_size();
}

/** [thread-safe] Return the alignment (in bytes) of the memory accesses on the
 *given shared memory region.
 * @param shared Shared memory region to query
 * @return Alignment used globally
 **/
size_t tm_align(shared_t shared) noexcept {
    return reinterpret_cast<Region *>(shared)->get_align();
}

/** [thread-safe] Begin a new transaction on the given shared memory region.
 * @param shared Shared memory region to start a transaction on
 * @param is_ro  Whether the transaction is read-only
 * @return Opaque transaction ID, 'invalid_tx' on failure
 **/
tx_t tm_begin(shared_t shared, bool is_ro) noexcept {
    Transaction *transaction = new Transaction(
        reinterpret_cast<Region *>(shared)->get_global_clock()->load(), is_ro);
    return reinterpret_cast<tx_t>(transaction);
}

/*
Lock the write-set: Acquire the locks in any convenient order using boun-
ded spinning to avoid indefinite deadlock. In case not all of these locks are
successfully acquired, the transaction fails.
*/
bool lock_write_set(Transaction *transaction) {
    unordered_set<Lock *> acquired_locks;
    bool rollback = false;
    for (const auto &write_pair : transaction->write_map) {
        if (write_pair.second.second->acquire_lock(transaction->read_version)) {
            acquired_locks.insert(write_pair.second.second);
        } else {
            rollback = true;
            break;
        }
    }

    if (rollback) {
        for (const auto &lock : acquired_locks) {
            lock->unlock();
        }
        return false;
    }
    return true;
}

/*
Validate the read-set: validate for each location in the read-set that the
version number associated with the versioned-write-lock is ≤ rv. We also
verify that these memory locations have not been locked by other threads.
In case the validation fails, the transaction is aborted. By
re-validating the read-set, we guarantee that its memory locations have
not been modified while steps 3 and 4 were being executed. In the special
case where rv + 1 = wv it is not necessary to validate the read-set, as
it is guaranteed that no concurrently executing transaction could have
modified it.
*/
bool validate_read_set(Transaction *transaction, int writeVersion) {
    if (transaction->read_version + 1 == writeVersion)
        return true;

    bool rollback = false;
    for (const auto &lock : transaction->read_set) {
        int version = lock->get_version();

        if (version == -1 || version > transaction->read_version) {
            rollback = true;
            break; // No need to check other locks; we're rolling back.
        }
    }
    if (rollback) {
        for (const auto &write_pair : transaction->write_map) {
            write_pair.second.second->unlock();
        }
        return false;
    }
    return true;
}

/** [thread-safe] End the given transaction.
 * @param shared Shared memory region associated with the transaction
 * @param tx     Transaction to end
 * @return Whether the whole transaction committed
 **/
bool tm_end(shared_t shared, tx_t tx) noexcept {
    Region *region = reinterpret_cast<Region *>(shared);
    Transaction *transaction = reinterpret_cast<Transaction *>(tx);

    if (transaction->is_read_only) {
        delete transaction;
        return true;
    }

    if (transaction->write_map.empty()) {
        delete transaction;
        return true;
    }

    if (!lock_write_set(transaction)) {
        delete transaction;
        return false;
    }

    int writeVersion = region->get_global_clock()->fetch_add(1) + 1;
    if (!validate_read_set(transaction, writeVersion)) {
        delete transaction;
        return false;
    }

    /*
    Commit and release the locks: For each location in the write-set, store
    to the location the new value from the write-set and release the locations
    lock by setting the version value to the write-version wv and clearing the
    write-lock bit (this is done using a simple store)
    */
    for (const auto &write_pair : transaction->write_map) {
        Word *word = region->get_word(write_pair.first);
        word->word = write_pair.second.first;
        word->lock.update_version(writeVersion);
    }
    delete transaction;
    return true;
}

inline bool handle_ro(Transaction *transaction, Region *region, size_t size,
                      void const *source, void *target) {
    size_t align = region->get_align();
    for (tx_t i = 0; i < size; i += align) {
        tx_t sourceAddress = reinterpret_cast<tx_t>(source) + i;

        Word *word = region->get_word(sourceAddress);
        int version_before = word->lock.get_version();

        if (unlikely(version_before == -1)) {
            delete transaction;
            return false;
        }

        uint64_t *targetWord = reinterpret_cast<uint64_t *>(target) + i;

        *targetWord = word->word;
        int version_after = word->lock.get_version();

        if (unlikely(version_before != version_after ||
                     version_after > transaction->read_version)) {
            delete transaction;
            return false;
        }
    }
    return true;
}

inline bool handle_not_ro(Transaction *transaction, Region *region, size_t size,
                          void const *source, void *target) {
    size_t align = region->get_align();
    for (tx_t i = 0; i < size; i += align) {
        tx_t sourceAddress = reinterpret_cast<tx_t>(source) + i;
        uint64_t *targetWord = reinterpret_cast<uint64_t *>(target) + i;

        auto found = transaction->write_map.find(sourceAddress);
        if (found != transaction->write_map.end()) {
            *targetWord = found->second.first;
            continue;
        }

        Word *word = region->get_word(sourceAddress);
        int version_before = word->lock.get_version();

        if (unlikely(version_before == -1 ||
                     version_before > transaction->read_version)) {
            delete transaction;
            return false;
        }

        *targetWord = word->word;

        int version_after = word->lock.get_version();

        if (unlikely(version_before != version_after)) {
            delete transaction;
            return false;
        }

        transaction->read_set.insert(&word->lock);
    }
    return true;
}

/** [thread-safe] Read operation in the given transaction, source in the shared
 *region and target in a private region.
 * @param shared Shared memory region associated with the transaction
 * @param tx     Transaction to use
 * @param source Source start address (in the shared region)
 * @param size   Length to copy (in bytes), must be a positive multiple of the
 *alignment
 * @param target Target start address (in a private region)
 * @return Whether the whole transaction can continue
 **/
bool tm_read(shared_t shared, tx_t tx, void const *source, size_t size,
             void *target) noexcept {
    Transaction *transaction = reinterpret_cast<Transaction *>(tx);
    Region *region = reinterpret_cast<Region *>(shared);
    if (!transaction->is_read_only) {
        return handle_not_ro(transaction, region, size, source, target);
    }

    return handle_ro(transaction, region, size, source, target);
}

/** [thread-safe] Write operation in the given transaction, source in a private
 *region and target in the shared region.
 * @param shared Shared memory region associated with the transaction
 * @param tx     Transaction to use
 * @param source Source start address (in a private region)
 * @param size   Length to copy (in bytes), must be a positive multiple of the
 *alignment
 * @param target Target start address (in the shared region)
 * @return Whether the whole transaction can continue
 **/
bool tm_write(shared_t shared, tx_t tx, void const *source, size_t size,
              void *target) noexcept {
    Transaction *transaction = reinterpret_cast<Transaction *>(tx);
    Region *region = reinterpret_cast<Region *>(shared);
    size_t align = region->get_align();
    for (size_t i = 0; i < size / align; i++) {
        tx_t targetWord = reinterpret_cast<tx_t>(target) + i * align;
        uint64_t value = *(reinterpret_cast<const uint64_t *>(source) + i);
        Word *word = region->get_word(targetWord);
        transaction->write_map[targetWord] = std::make_pair(value, &word->lock);
    }

    return true;
}

/** [thread-safe] Memory allocation in the given transaction.
 * @param shared Shared memory region associated with the transaction
 * @param tx     Transaction to use
 * @param size   Allocation requested size (in bytes), must be a positive
 *multiple of the alignment
 * @param target Pointer in private memory receiving the address of the first
 *byte of the newly allocated, aligned segment
 * @return Whether the whole transaction can continue (success/nomem), or not
 *(abort_alloc)
 **/
Alloc tm_alloc(shared_t shared, tx_t unused(tx), size_t unused(size),
               void **target) noexcept {
    *target = reinterpret_cast<Region *>(shared)->get_next_free_segment();
    return Alloc::success;
}

/** [thread-safe] Memory freeing in the given transaction.
 * @param shared Shared memory region associated with the transaction
 * @param tx     Transaction to use
 * @param target Address of the first byte of the previously allocated segment
 *to deallocate
 * @return Whether the whole transaction can continue
 **/
bool tm_free(shared_t unused(shared), tx_t unused(tx),
             void *unused(target)) noexcept {
    return true;
}
