#include <papi.h>
#include <stdbool.h>

int library_init();
int register_thread();
int start_counters();
long long* stop_counters();
