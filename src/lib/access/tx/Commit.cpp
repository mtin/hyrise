#include "Commit.h"

#include "access/system/QueryParser.h"
#include "io/TransactionManager.h"

namespace hyrise { namespace access {

namespace {
  auto _ = QueryParser::registerPlanOperation<Commit>("Commit");
}

void Commit::executePlanOperation() {
  tx::TransactionManager::commitTransaction(_txContext, _flush_log);
  
  // add all input data to the output
  for (size_t i=0; i<input.numberOfTables(); ++i) {
  	addResult(input.getTable(i));
  }
}

void Commit::setFlushLog(bool flush_log) {
	_flush_log = flush_log;
}

std::shared_ptr<PlanOperation> Commit::parse(const Json::Value &data) {
  auto c = std::make_shared<Commit>();
  c->setFlushLog(data["flush_log"].asBool());
  return c;
}

}}
