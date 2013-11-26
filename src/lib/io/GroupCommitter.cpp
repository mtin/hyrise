// Copyright (c) 2012 Hasso-Plattner-Institut fuer Softwaresystemtechnik GmbH. All rights reserved.

#include <io/GroupCommitter.h>

#include <helper/HwlocHelper.h>
#include <io/logging.h>

#include <sys/time.h>
#include <iostream>

namespace hyrise {
namespace io {

GroupCommitter& GroupCommitter::getInstance() {
	static GroupCommitter g;
	return g;
};

void GroupCommitter::push(ENTRY_T entry) {
	_queue.push(entry);
}

GroupCommitter::GroupCommitter() : _running(true) {
	_thread = std::thread(&GroupCommitter::run, this);
	int COMMITTER_CORE = 1;

    hwloc_topology_t topology = getHWTopology();
    hwloc_obj_t obj = hwloc_get_obj_by_type(topology, HWLOC_OBJ_CORE, COMMITTER_CORE);
    hwloc_cpuset_t cpuset = hwloc_bitmap_dup(obj->cpuset);
  	hwloc_bitmap_singlify(cpuset);
	if (hwloc_set_thread_cpubind(topology, _thread.native_handle(), cpuset, HWLOC_CPUBIND_STRICT | HWLOC_CPUBIND_NOMEMBIND)) {
        char *str;
        int error = errno;
        hwloc_bitmap_asprintf(&str, obj->cpuset);
        fprintf(stderr, "Couldn't bind GroupCommitter to cpuset %s: %s\n", str, strerror(error));
        fprintf(stderr, "Continuing as normal, however, no guarantees\n");
		free(str);
    }
    hwloc_bitmap_free(cpuset);
}

GroupCommitter::~GroupCommitter() {
	_running = false;
	_thread.join();
}

void GroupCommitter::run() {
	struct timeval time_start, time_cur;
	long long elapsed;
	gettimeofday(&time_start, NULL);

	while(_running) {
		ENTRY_T tmp;
		while(_queue.try_pop(tmp)) {
			_toBeFlushed.push_back(tmp);
		}

		gettimeofday(&time_cur, NULL);
		elapsed = (time_cur.tv_sec*1000000 + time_cur.tv_usec) - (time_start.tv_sec*1000000 + time_start.tv_usec);

		if(elapsed > GROUP_COMMIT_WINDOW) {
			#ifndef COMMIT_WITHOUT_FLUSH
			Logger::getInstance().flush();
			#endif
			respondClients();
			time_start = time_cur;
		}
	}
}

void GroupCommitter::respondClients() {
	for(ENTRY_T &entry: _toBeFlushed) {
		net::AbstractConnection* connection;
		size_t status;
		std::string response;
		std::tie(connection, status, response) = entry;
	    connection->respond(response, status);
	}
	_toBeFlushed.clear();
}


}}
