#pragma once

#include "lock.hpp"
#include "tm.hpp"
#include "transaction.hpp"
#include <array>
#include <atomic>

/*
The Region class represents a shared memory region.
It maintains an atomic global clock.
Memory is divided into segments, with each segment represented by a Word.
*/
class Region {
  public:
    atomic_int global_clock;
    atomic_int next_free_segment_counter;
    int size;
    int align;
    array<array<Word, 4096>, 1024>
        memory; // TODO: this is too much, but doesn't matter?

  public:
    Region() : global_clock(0), next_free_segment_counter(1){};
    Region(size_t size, size_t align)
        : global_clock(0), next_free_segment_counter(1), size(size),
          align(align){};

    int get_size() { return size; }

    int get_align() { return align; }

    atomic_int *get_global_clock() { return &global_clock; }

    Word *get_word(uint64_t addr) {
        return &memory[((addr & 0xFFFF000000000000) >> 48) - 1]
                      [(addr & 0xFFFFFFFFFFFF) / align];
    }

    void *get_first_segment() { return (void *)((uint64_t)1 << 48); }

    void *get_next_free_segment() {
        uint64_t counter = atomic_fetch_add(&next_free_segment_counter, 1) + 1;
        return (void *)(counter << 48);
    }
};
