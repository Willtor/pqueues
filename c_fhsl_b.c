/* Fixed height skiplist: A skiplist implementation with an array of "next"
 * nodes of fixed height.
 */

#include "c_fhsl_b.h"
#include "utils.h"

#include <stdatomic.h>
#include <stdbool.h>
#include <forkscan.h>
#include <stdio.h>
#include <pthread.h>


#define BOTTOM 0

struct c_fhsl_b_t {
  node_t head, tail;
};


static node_ptr node_create(int64_t key, int32_t toplevel){
  node_ptr node = forkscan_malloc(sizeof(node_t));
  node->key = key;
  node->toplevel = toplevel;
  atomic_store_explicit(&node->marked, false, memory_order_relaxed);
  atomic_store_explicit(&node->fully_linked, false, memory_order_relaxed);
  pthread_spin_init(&node->lock, PTHREAD_PROCESS_PRIVATE);
  return node;
}

static void unlock_nodes(node_ptr preds[N], int32_t highest_locked) {
  for(int32_t i = BOTTOM; i <= highest_locked; i++) {
    pthread_spin_unlock(&preds[i]->lock);
  }
}

bool ok_to_delete(node_ptr candidate)
{
  bool fully_linked = atomic_load_explicit(&candidate->fully_linked, memory_order_relaxed);
  bool is_marked = atomic_load_explicit(&candidate->marked, memory_order_relaxed);
  return fully_linked && !is_marked;
}

void c_fhsl_b_print (c_fhsl_b_t *set){
  node_ptr node = atomic_load_explicit(&set->head.next[BOTTOM], memory_order_consume);
  while(node != &set->tail) {
    node_ptr next = atomic_load_explicit(&node->next[BOTTOM], memory_order_consume);
    printf("node[%d]: %ld\n", node->toplevel, node->key);
    node = next;
  }
}

c_fhsl_b_t * c_fhsl_b_create() {
  c_fhsl_b_t* fhsl_b = forkscan_malloc(sizeof(c_fhsl_b_t));
  fhsl_b->head.key = INT64_MIN;
  fhsl_b->tail.key = INT64_MAX;
  atomic_store_explicit(&fhsl_b->head.marked, false, memory_order_relaxed);
  atomic_store_explicit(&fhsl_b->tail.marked, false, memory_order_relaxed);
  atomic_store_explicit(&fhsl_b->head.fully_linked, true, memory_order_relaxed);
  atomic_store_explicit(&fhsl_b->tail.fully_linked, true, memory_order_relaxed);
  for(int64_t i = 0; i < N; i++) {
    atomic_store_explicit(&fhsl_b->head.next[i], &fhsl_b->tail, memory_order_relaxed);
    atomic_store_explicit(&fhsl_b->tail.next[i], NULL, memory_order_relaxed);
  }
  pthread_spin_init(&fhsl_b->head.lock, PTHREAD_PROCESS_PRIVATE);
  pthread_spin_init(&fhsl_b->tail.lock, PTHREAD_PROCESS_PRIVATE);
  return fhsl_b;
}


int c_fhsl_b_contains(c_fhsl_b_t *set, int64_t key) {
  node_ptr node = &set->head;
  for(int64_t i = N - 1; i >= 0; i--) {
    node_ptr next = atomic_load_explicit(&node->next[i], memory_order_consume);
    while(next->key <= key) {
      node = next; 
      next = atomic_load_explicit(&node->next[i], memory_order_consume);
    }
    if(node->key == key) {
      bool linked = atomic_load_explicit(&node->fully_linked, memory_order_relaxed);
      bool marked = atomic_load_explicit(&node->marked, memory_order_relaxed);
      return linked && !marked;
    }
  }
  return false;
}

int c_fhsl_b_contains_serial(c_fhsl_b_t *set, int64_t key) {
  node_ptr node = &set->head;
  for(int64_t i = N - 1; i >= 0; i--) {
    node_ptr next = atomic_load_explicit(&node->next[i], memory_order_relaxed);
    while(next->key <= key) {
      node = next; 
      next = atomic_load_explicit(&node->next[i], memory_order_relaxed);
    }
    if(node->key == key) {
      return true;
    }
  }
  return false;
}


static bool find_serial(c_fhsl_b_t *set, int64_t key, 
  node_ptr preds[N], node_ptr succs[N]) {
  node_ptr left = &set->head, right = left;
  for(int64_t level = N - 1; level >= BOTTOM; --level) {
    right = atomic_load_explicit(&left->next[level], memory_order_consume);
    while(right->key < key) {
      left = right;
      right = atomic_load_explicit(&right->next[level], memory_order_consume);
    }
    preds[level] = left;
    succs[level] = right;
  }
  return succs[BOTTOM]->key == key;
}

int c_fhsl_b_add(uint64_t *seed, c_fhsl_b_t *set, int64_t key) {
  node_ptr preds[N], succs[N];
  int32_t toplevel = random_level(seed, N);
  node_ptr node = NULL;
  while(true) {
    if(find_serial(set, key, preds, succs)) {
      forkscan_free((void*)node);
      node_ptr found_node = succs[BOTTOM];
      bool marked = atomic_load_explicit(&found_node->marked, memory_order_relaxed);
      if(!marked) {
        while(!atomic_load_explicit(&found_node->fully_linked, memory_order_relaxed)) {}
        return false;
      }
      continue;
    }
    // Lock all our nodes.
    int32_t highest_locked = -1;
    node_ptr prev = NULL;
    bool valid = true;
    for(size_t i = BOTTOM; i <= toplevel && valid; i++) {
      node_ptr left = preds[i];
      node_ptr right = succs[i];
      if(left != prev) {
        pthread_spin_lock(&left->lock);
        highest_locked = i;
        prev = left;
      }
      bool left_marked = atomic_load_explicit(&left->marked, memory_order_relaxed);
      bool right_marked = atomic_load_explicit(&right->marked, memory_order_relaxed);
      node_ptr left_next = atomic_load_explicit(&left->next[i], memory_order_relaxed);
      valid = (!left_marked && !right_marked && left_next == right);
    }
    if(!valid) {
      unlock_nodes(preds, highest_locked);
      continue;
    }
    if(node == NULL) { node = node_create(key, toplevel); }
    for(size_t i = BOTTOM; i <= toplevel && valid; i++) {
      atomic_store_explicit(&node->next[i], succs[i], memory_order_release);
      atomic_store_explicit(&preds[i]->next[i], node, memory_order_release);
    }
    atomic_store_explicit(&node->fully_linked, true, memory_order_relaxed);
    unlock_nodes(preds, highest_locked);
    return true;
  }
}

int c_fhsl_b_add_serial(uint64_t *seed, c_fhsl_b_t *set, int64_t key) {
  node_ptr preds[N], succs[N];
  int32_t toplevel = -1;
  node_ptr node = NULL;
  if(find_serial(set, key, preds, succs)) {
    forkscan_free((void*)node);
    return false;
  }
  if(node == NULL) { 
    toplevel = random_level(seed, N);
    node = node_create(key, toplevel); 
  }
  for(int64_t i = BOTTOM; i <= toplevel; ++i) {
    atomic_store_explicit(&node->next[i], succs[i], memory_order_release);
    atomic_store_explicit(&preds[i]->next[i], node, memory_order_release);
  }
  return true;
}

int c_fhsl_b_remove_leaky(c_fhsl_b_t *set, int64_t key) {
  node_ptr preds[N], succs[N];
  bool is_marked = false;
  node_ptr deleted_node = NULL;
  while(true) {
    bool found = find_serial(set, key, preds, succs);
    if(is_marked || (found && ok_to_delete(succs[BOTTOM]))) {
      if(!is_marked) {
        deleted_node = succs[BOTTOM];
        pthread_spin_lock(&deleted_node->lock);
        bool marked = atomic_load_explicit(&deleted_node->marked, memory_order_relaxed);
        if(marked) {
          pthread_spin_unlock(&deleted_node->lock);
          return false;
        }
        atomic_store_explicit(&deleted_node->marked, true, memory_order_relaxed);
        is_marked = true;
      }
      // Lock all our nodes.
      int32_t highest_locked = -1;
      node_ptr prev = NULL;
      bool valid = true;
      for(size_t i = BOTTOM; i <= deleted_node->toplevel && valid; i++) {
        node_ptr left = preds[i];
        node_ptr right = succs[i];
        if(left != prev) {
          pthread_spin_lock(&left->lock);
          highest_locked = i;
          prev = left;
        }
        bool left_marked = atomic_load_explicit(&left->marked, memory_order_relaxed);
        node_ptr left_next = atomic_load_explicit(&left->next[i], memory_order_relaxed);
        valid = (!left_marked && left_next == right);
      }
      if(!valid) {
        unlock_nodes(preds, highest_locked);
        continue;
      }
      for(size_t i = BOTTOM; i <= deleted_node->toplevel && valid; i++) {
        node_ptr next = atomic_load_explicit(&deleted_node->next[i], memory_order_consume);
        atomic_store_explicit(&preds[i]->next[i], next, memory_order_release);
      }
      pthread_spin_unlock(&deleted_node->lock);
      unlock_nodes(preds, highest_locked);
      return true;
    } else {
      return false;
    }
  }
}

int c_fhsl_b_remove_leaky_serial(c_fhsl_b_t *set, int64_t key) {
  node_ptr preds[N], succs[N];
  if(!find_serial(set, key, preds, succs)) {
    return false;
  }
  node_ptr node = succs[BOTTOM];
  for(int64_t i = BOTTOM; i <= node->toplevel; ++i) {
    node_ptr next = atomic_load_explicit(&node->next[i], memory_order_relaxed);
    atomic_store_explicit(&preds[i]->next[i], next, memory_order_relaxed);
  }
  return true;
}


int c_fhsl_b_remove(c_fhsl_b_t *set, int64_t key) {
  node_ptr preds[N], succs[N];
  bool is_marked = false;
  node_ptr deleted_node = NULL;
  while(true) {
    bool found = find_serial(set, key, preds, succs);
    if(is_marked || (found && ok_to_delete(succs[BOTTOM]))) {
      if(!is_marked) {
        deleted_node = succs[BOTTOM];
        pthread_spin_lock(&deleted_node->lock);
        bool marked = atomic_load_explicit(&deleted_node->marked, memory_order_relaxed);
        if(marked) {
          pthread_spin_unlock(&deleted_node->lock);
          return false;
        }
        atomic_store_explicit(&deleted_node->marked, true, memory_order_relaxed);
        is_marked = true;
      }
      // Lock all our nodes.
      int32_t highest_locked = -1;
      node_ptr prev = NULL;
      bool valid = true;
      for(size_t i = BOTTOM; i <= deleted_node->toplevel && valid; i++) {
        node_ptr left = preds[i];
        node_ptr right = succs[i];
        if(left != prev) {
          pthread_spin_lock(&left->lock);
          highest_locked = i;
          prev = left;
        }
        bool left_marked = atomic_load_explicit(&left->marked, memory_order_relaxed);
        bool right_marked = atomic_load_explicit(&right->marked, memory_order_relaxed);
        node_ptr left_next = atomic_load_explicit(&left->next[i], memory_order_relaxed);
        valid = (!left_marked && !right_marked && left_next == right);
      }
      if(!valid) {
        unlock_nodes(preds, highest_locked);
        continue;
      }
      for(size_t i = BOTTOM; i <= deleted_node->toplevel && valid; i++) {
        node_ptr next = atomic_load_explicit(&deleted_node->next[i], memory_order_consume);
        atomic_store_explicit(&preds[i]->next[i],next, memory_order_release);
      }
      pthread_spin_unlock(&deleted_node->lock);
      unlock_nodes(preds, highest_locked);
      forkscan_retire(deleted_node);
      return true;
    } else {
      return false;
    }
  }
}

int c_fhsl_b_remove_serial(c_fhsl_b_t *set, int64_t key) {
  node_ptr preds[N], succs[N];
  if(!find_serial(set, key, preds, succs)) {
    return false;
  }
  node_ptr node = succs[BOTTOM];
  for(int64_t i = BOTTOM; i <= node->toplevel; ++i) {
    node_ptr next = atomic_load_explicit(&node->next[i], memory_order_consume);
    atomic_store_explicit(&preds[i]->next[i], next, memory_order_release);
  }
  forkscan_retire(node);
  return true;
}

int c_fhsl_b_pop_min_leaky (c_fhsl_b_t *set) {
  pthread_spin_lock(&set->head.lock);
  node_ptr node_to_remove = atomic_load_explicit(&set->head.next[BOTTOM], memory_order_consume);
  if(node_to_remove == &set->tail) {
    pthread_spin_unlock(&set->head.lock);
    return false;
  }
  for(int32_t i = BOTTOM; i <= node_to_remove->toplevel; i++) {
    node_ptr next = atomic_load_explicit(&node_to_remove->next[i], memory_order_consume);
    atomic_store_explicit(&set->head.next[i], next, memory_order_release);
  }
  pthread_spin_unlock(&set->head.lock);
  return true;
}

int c_fhsl_b_pop_min_leaky_serial (c_fhsl_b_t *set) {
  node_ptr head_node = atomic_load_explicit(&set->head.next[BOTTOM], memory_order_consume);
  if(head_node != &set->tail) {
    node_ptr node_popped = head_node;
    int64_t toplevel = node_popped->toplevel;
    for(int64_t i = BOTTOM; i <= toplevel; i++) {
      set->head.next[i] = node_popped->next[i];
    }
    return true;
  }
  return false;
}

int c_fhsl_b_pop_min(c_fhsl_b_t *set) {
  pthread_spin_lock(&set->head.lock);
  node_ptr node_to_remove = atomic_load_explicit(&set->head.next[BOTTOM], memory_order_consume);
  if(node_to_remove == &set->tail) {
    pthread_spin_unlock(&set->head.lock);
    return false;
  }
  for(int32_t i = BOTTOM; i <= node_to_remove->toplevel; i++) {
    node_ptr next = atomic_load_explicit(&node_to_remove->next[i], memory_order_consume);
    atomic_store_explicit(&set->head.next[i], next, memory_order_release);
  }
  forkscan_retire(node_to_remove);
  pthread_spin_unlock(&set->head.lock);
  return true;
}

int c_fhsl_b_pop_min_serial (c_fhsl_b_t *set) {
  node_ptr head_node = atomic_load_explicit(&set->head.next[BOTTOM], memory_order_consume);
  if(head_node != &set->tail) {
    node_ptr node_popped = head_node;
    int64_t toplevel = node_popped->toplevel;
    for(int64_t i = BOTTOM; i <= toplevel; i++) {
      node_ptr next = atomic_load_explicit(&node_popped->next[i], memory_order_consume);
      atomic_store_explicit(&set->head.next[i], next, memory_order_release);
    }
    forkscan_retire(node_popped);
    return true;
  }
  return false;
}

/*
  Don't use this in conjunction with concurrent remove calls. Will break.
*/
int c_fhsl_b_bulk_pop(c_fhsl_b_t *set, size_t amount, node_ptr *head, node_ptr *tail) {
  node_ptr local_head = atomic_load_explicit(&set->head.next[BOTTOM], memory_order_consume);
  if(local_head == &set->tail) {
    *head = *tail = NULL;
    return 0;
  }
  int approx_seen = 0;
  node_ptr local_tail = local_head;
  for(size_t i = 0; i < amount; i++, approx_seen++) {
    node_ptr next = atomic_load_explicit(&local_tail->next[BOTTOM], memory_order_consume);
    if(next == &set->tail) {
      break;
    }
    local_tail = next;
  }
  node_ptr next = atomic_load_explicit(&local_tail->next[BOTTOM], memory_order_consume);
  node_ptr preds[N], succs[N];
  bool found = find_serial(set, next->key, preds, succs);
  pthread_spin_lock(&set->head.lock);
  for(int32_t i = BOTTOM; i < N; i++) {
    atomic_store_explicit(&set->head.next[i], succs[i], memory_order_release);
  }
  pthread_spin_unlock(&set->head.lock);
  *head = local_head;
  *tail = local_tail;
  return approx_seen;
}

void c_fhsl_b_bulk_push(c_fhsl_b_t *set, node_ptr head, node_ptr tail) {
  node_ptr preds[N], succs[N];
  bool _ = find_serial(set, INT64_MAX, preds, succs);
  node_ptr current = head;
  for(int32_t i = BOTTOM; i < tail->toplevel; i++) {
    atomic_store_explicit(&tail->next[i], &set->tail, memory_order_release);
  }
  int32_t linkup_height = 0;
  while(current != tail) {
    if(linkup_height <= current->toplevel) {
      for(int32_t i = linkup_height; i <= current->toplevel; i++, linkup_height++) {
        atomic_store_explicit(&preds[i]->next[i], current, memory_order_release);
      }
    }
    // Our mission is done here!
    if(linkup_height >= N) {
      return;
    }
    node_ptr next = atomic_load_explicit(&current->next[BOTTOM], memory_order_consume);
    current = next;
  }
}