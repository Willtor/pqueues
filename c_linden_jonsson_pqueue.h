#pragma once

#include <stdint.h>

#define N 20

enum LJ_STATE { INSERT_PENDING, INSERTED };
typedef enum LJ_STATE lj_state_t;

typedef struct _linden_jonsson_node_t {
  int64_t key;
  int32_t toplevel;
  volatile lj_state_t insert_state;
  struct _linden_jonsson_node_t volatile * volatile next[N];
} c_linden_jonsson_node_t;

typedef c_linden_jonsson_node_t volatile * volatile c_linden_jonsson_node_ptr;

typedef struct _c_linden_jonsson_pqueue_t {
  uint32_t boundoffset;
  c_linden_jonsson_node_t head, tail;
} c_linden_jonsson_pqueue_t;

c_linden_jonsson_pqueue_t * c_linden_jonsson_pqueue_create(uint32_t boundoffset);

int c_linden_jonsson_pqueue_contains(c_linden_jonsson_pqueue_t * set, int64_t key);
int c_linden_jonsson_pqueue_add(uint64_t *seed, c_linden_jonsson_pqueue_t * set, int64_t key);
int c_linden_jonsson_pqueue_remove_leaky(c_linden_jonsson_pqueue_t * set, int64_t key);
int c_linden_jonsson_pqueue_leaky_pop_min(c_linden_jonsson_pqueue_t * set);
void c_linden_jonsson_pqueue_print (c_linden_jonsson_pqueue_t *set);