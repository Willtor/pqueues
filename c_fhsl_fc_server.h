#pragma once

#include <stdint.h>
#include <stddef.h>

typedef struct c_fhsl_fc_server_t c_fhsl_fc_server_t;

c_fhsl_fc_server_t * c_fhsl_fc_server_create(size_t num_threads);

int c_fhsl_fc_server_contains(c_fhsl_fc_server_t * set, int64_t key, size_t thread_id);
int c_fhsl_fc_server_add(c_fhsl_fc_server_t * set, int64_t key, size_t thread_id);
int c_fhsl_fc_server_remove(c_fhsl_fc_server_t * set, int64_t key, size_t thread_id);
int c_fhsl_fc_server_pop_min(c_fhsl_fc_server_t *set, size_t thread_id);
void c_fhsl_fc_server_print (c_fhsl_fc_server_t *set);