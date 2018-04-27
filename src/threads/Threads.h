#ifndef THREADS_H
#define THREADS_H
#include <thread>
#include <vector>
void set_thread_affinity(std::vector<std::thread>& threads);
#endif

