/* Lock-free binary tree..
 * From paper "Fast Concurrent Lock-Free Binary Search Trees"
 * Lock-free updates (add/remove) and wait-free reads (contains).
*/

#pragma once

#include <stdint.h>
#include <stdbool.h>
#include <forkscan.h>

typedef struct _bt_lf_node_t {
    int64_t key;
    volatile struct _bt_lf_node_t * volatile left, * volatile right;
} bt_lf_node_t;

typedef bt_lf_node_t volatile * volatile bt_lf_node_ptr;


typedef struct _bt_lf_t {
    bt_lf_node_ptr R, S;
} bt_lf_t;

bt_lf_t * bt_lf_create();

int bt_lf_contains(bt_lf_t * set, int64_t key);
int bt_lf_add(bt_lf_t * set, int64_t key);
int bt_lf_remove_leaky(bt_lf_t * set, int64_t key);
int bt_lf_remove_retire(bt_lf_t * set, int64_t key);