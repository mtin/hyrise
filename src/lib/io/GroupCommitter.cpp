// Copyright (c) 2012 Hasso-Plattner-Institut fuer Softwaresystemtechnik GmbH. All rights reserved.

#include "GroupCommitter.h"
#include "io/logging.h"
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

void GroupCommitter::run() {
	std::vector<ENTRY_T> entries_getting_flushed;
	while(true) {
		ENTRY_T tmp;
		while(_queue.try_pop(tmp)) {
			entries_getting_flushed.push_back(tmp);
		}
		if(entries_getting_flushed.empty()) {
			usleep(10);
			continue;
		}
		Logger::getInstance().flush();
		for(ENTRY_T entry: entries_getting_flushed) {
			net::AbstractConnection* connection;
			size_t status;
			std::string response;
			std::tie(connection, status, response) = entry;
		    connection->respond(response, status);
		}
		entries_getting_flushed.clear();
	}
 }

GroupCommitter::GroupCommitter() {
	_thread = std::thread(&GroupCommitter::run, this);
}

GroupCommitter::~GroupCommitter() {
	_thread.join();
}

}}
