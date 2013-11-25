// Copyright (c) 2012 Hasso-Plattner-Institut fuer Softwaresystemtechnik GmbH. All rights reserved.
#ifndef SRC_LIB_IO_GROUPCOMMITER_H
#define SRC_LIB_IO_GROUPCOMMITER_H

#include "tbb/concurrent_queue.h"
#include "net/AbstractConnection.h"
#include "taskscheduler/Task.h"

namespace hyrise {
namespace io {

class GroupCommitter {
public:
	typedef std::tuple<net::AbstractConnection*, size_t, std::string> ENTRY_T;

	static GroupCommitter& getInstance();
	void push(ENTRY_T entry);
	void run();

private:
	GroupCommitter();
	~GroupCommitter();
	std::thread _thread;
	tbb::concurrent_queue<ENTRY_T> _queue;
};

}}

#endif