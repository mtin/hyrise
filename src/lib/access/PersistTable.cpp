// Copyright (c) 2012 Hasso-Plattner-Institut fuer Softwaresystemtechnik GmbH. All rights reserved.
#include <access/PersistTable.h>

#include <io/StorageManager.h>

namespace hyrise {
namespace access {

namespace {
auto _ = QueryParser::registerPlanOperation<PersistTable>("PersistTable");
}

void PersistTable::executePlanOperation() {
  if (_tableName.empty()) {
    throw std::runtime_error("PersistTable requires a table name");
  }
  io::StorageManager::getInstance()->persistTable(_tableName);
}

std::shared_ptr<PlanOperation> PersistTable::parse(const Json::Value &data) {
  auto pt = BasicParser<PersistTable>::parse(data);
  pt->setTableName(data["table_name"].asString());
  return pt;
}

const std::string PersistTable::vname() {
  return "PersistTable";
}

void PersistTable::setTableName(const std::string &name) {
  _tableName = name;
}

}
}
