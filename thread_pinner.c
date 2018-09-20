#define _GNU_SOURCE
#include "cpuinfo.h"
#include "thread_pinner.h"
#include <assert.h>
#include <sched.h>
#include <stdint.h>
#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>

typedef struct socket_t socket_t;

struct thread_pinner_t {
  uint32_t current_socket, num_sockets;
  socket_t *sockets;
};

struct socket_t {
  uint32_t socket_id;
  uint32_t current_processor, num_processors;
  uint32_t *processor_queue;
};

static bool find(uint32_t *cores, uint32_t num_cores, uint32_t entry){
  for(uint32_t i = 0; i < num_cores; i++) {
    if(cores[i] == entry) {
      return true;
    }
  }
  return false;
}

static void populate_socket(socket_t *my_socket, const struct cpuinfo_cluster * socket) {
  uint32_t middle_index = my_socket->num_processors / 2;
  uint32_t first_offset = 1, second_offset = 0;
  for (uint32_t current = socket->processor_start;
        current != socket->processor_start + socket->processor_count;
        current++) {
    const struct cpuinfo_cache *current_l2 =
        cpuinfo_get_processor(current)->cache.l2;
    bool first = true;
    for (uint32_t processor = 0; processor < current_l2->processor_count;
          processor++, first = !first) {
      uint32_t processor_linux_id =
          cpuinfo_get_processor(current_l2->processor_start + processor)->linux_id;
      if (find(my_socket->processor_queue, my_socket->num_processors, processor_linux_id)) {
        continue;
      }
      if (first) {
        my_socket->processor_queue[middle_index - first_offset++] = processor_linux_id;
      } else {
        my_socket->processor_queue[middle_index + second_offset++] = processor_linux_id;
      }
    }
  }
}

static void print_socket(socket_t *my_socket) {
  printf("********************\n");
  printf("Socket number: %d\n", my_socket->socket_id);
  for(uint32_t i = 0; i < my_socket->num_processors; i++) {
    printf("Processor id: %d\n", my_socket->processor_queue[i]);
  }
  printf("********************\n");
}

thread_pinner_t * thread_pinner_create() {
  assert(cpuinfo_initialize());
  thread_pinner_t * pinner = malloc(sizeof(thread_pinner_t));
  pinner->current_socket = 0;
  pinner->num_sockets = cpuinfo_get_clusters_count();
  pinner->sockets = malloc(sizeof(socket_t) * pinner->num_sockets);
  const struct cpuinfo_cluster *sockets = cpuinfo_get_clusters();
  for(uint32_t i = 0; i < pinner->num_sockets; i++) {
    const struct cpuinfo_cluster *socket = sockets + i;
    pinner->sockets[i].current_processor = 0;
    pinner->sockets[i].num_processors = socket->processor_count;
    pinner->sockets[i].processor_queue = 
      malloc(sizeof(uint32_t) * socket->processor_count);
    for(uint32_t j = 0; j < socket->processor_count; j++) {
      pinner->sockets[i].processor_queue[j] = UINT32_MAX;
    }
  }
  
  uint32_t num_sockets = cpuinfo_get_clusters_count();
  // printf("Num sockets: %d\n", num_sockets);
  for(uint32_t current_socket = 0;
    current_socket < num_sockets;
    current_socket++) {
      const struct cpuinfo_cluster *socket = sockets + current_socket;
      pinner->sockets[current_socket].socket_id = current_socket;
      populate_socket(&pinner->sockets[current_socket], socket);
      // print_socket(&pinner->sockets[current_socket]);
  }
  return pinner;
}

static int pin_thread_to_socket(socket_t *socket, pthread_t thread) {
  if(socket->current_processor == socket->num_processors) { return false; }
  cpu_set_t cpu_set;
  CPU_ZERO(&cpu_set);
  uint32_t core_id = socket->processor_queue[socket->current_processor++];
  CPU_SET(core_id, &cpu_set);
  // printf("Pinning thread to processor: %d\n", core_id);
  return pthread_setaffinity_np(thread, sizeof(cpu_set_t), &cpu_set) == 0;
}

int get_num_cores() {
  return sysconf(_SC_NPROCESSORS_ONLN);
}

int pin_thread(thread_pinner_t * thread_pinner, pthread_t thread) {
  for(uint32_t current_socket = 0;
    current_socket < thread_pinner->num_sockets;
    current_socket++) {
    if(pin_thread_to_socket(&thread_pinner->sockets[current_socket],
      thread)) { return 0; }
  }
  return 1;
}