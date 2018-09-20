#include "utils.h"

uint64_t* fetch_and_or(uint64_t* ptr, uint64_t mark) {
  return (uint64_t*)__sync_fetch_and_or(ptr, mark);
}