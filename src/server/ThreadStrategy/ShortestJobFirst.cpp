#include "ShortestJobFirst.h"
#include "server/Worker.h"
#include <iostream>
#include <algorithm>



namespace ldbc_server {
namespace thread_strategy {
void ShortestJobFirst::next(Worker::Task job) {
	if (workers_.size() == 0) {
		return;
	}
	//auto worker = std::min_element(workers_.begin(), workers_.end(), [](Worker& worker_a, Worker& worker_b){ return worker_a.num_jobs() < worker_b.num_jobs(); });
	auto worker = std::min_element(workers_.begin(), workers_.end(), [](Worker* worker_a, Worker* worker_b){
	if ( worker_a->is_unemployed() == worker_b->is_unemployed() ) 
		return worker_a->id() < worker_b->id();
	return worker_a->is_unemployed();

 });
	(*worker)->add_job(job);
}
}
}
