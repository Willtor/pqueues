#pragma once

#include <stdint.h>

uint64_t* fetch_and_or(uint64_t *, uint64_t);
uint64_t fast_rand (uint64_t *seed);
int32_t random_level (uint64_t *seed, int32_t max);