// Copyright (c) 2012 Hasso-Plattner-Institut fuer Softwaresystemtechnik GmbH. All rights reserved.
#include <access/RecoverTable.h>

#include <io/StorageManager.h>

namespace hyrise {
namespace access {

namespace {
auto _ = QueryParser::registerPlanOperation<RecoverTable>("RecoverTable");
}

void RecoverTable::executePlanOperation() {
  if (_tableName.empty()) {
    throw std::runtime_error("RecoverTable requires a table name");
  }
  io::StorageManager::getInstance()->recoverTable(_tableName);
}

std::shared_ptr<PlanOperation> RecoverTable::parse(const Json::Value &data) {
  auto rt = BasicParser<RecoverTable>::parse(data);
  rt->setTableName(data["table_name"].asString());
  return rt;
}

const std::string RecoverTable::vname() {
  return "RecoverTable";
}

void RecoverTable::setTableName(const std::string &name) {
  _tableName = name;
}

}
}
