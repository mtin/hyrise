#include "Commit.h"

#include "access/system/QueryParser.h"
#include "io/TransactionManager.h"

namespace hyrise { namespace access {

namespace {
  auto _ = QueryParser::registerPlanOperation<Commit>("Commit");
}

void Commit::executePlanOperation() {
  tx::TransactionManager::commitTransaction(_txContext);
  
  // add all input data to the output
  for (size_t i=0; i<input.numberOfTables(); ++i)
  	addResult(input.getTable(0));
}

std::shared_ptr<PlanOperation> Commit::parse(Json::Value &data) {
  return std::make_shared<Commit>();
}

}}
