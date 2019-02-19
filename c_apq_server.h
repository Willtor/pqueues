#pragma once

#include <stdint.h>
#include <stddef.h>

typedef struct c_apq_server_t c_apq_server_t;

c_apq_server_t * c_apq_server_create(size_t num_threads, int64_t cutoff_key);

int c_apq_server_add(uint64_t *seed, c_apq_server_t * set, int64_t key, size_t thread_id);
int c_apq_server_pop_min_leaky(c_apq_server_t *set, size_t thread_id);
int c_apq_server_pop_min(c_apq_server_t *set, size_t thread_id);
void c_apq_server_print (c_apq_server_t *set);