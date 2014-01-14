// Copyright (c) 2012 Hasso-Plattner-Institut fuer Softwaresystemtechnik GmbH. All rights reserved.
#include "access/CreateGroupkeyIndex.h"

#include <string>
#include <vector>
#include <map>

#include "access/system/BasicParser.h"
#include "access/system/QueryParser.h"

#include "io/StorageManager.h"

#include "storage/AbstractTable.h"
#include "storage/meta_storage.h"
#include "storage/storage_types.h"
#include "storage/PointerCalculator.h"
#include "storage/AbstractIndex.h"
#include "storage/GroupkeyIndex.h"

namespace hyrise {
namespace access {

struct CreateGroupkeyIndexFunctor {
  typedef std::shared_ptr<storage::AbstractIndex> value_type;
  const storage::c_atable_ptr_t& in;
  size_t column;

  CreateGroupkeyIndexFunctor(const storage::c_atable_ptr_t& t, size_t c):
    in(t), column(c) {}

  template<typename R>
  value_type operator()() {
    return std::make_shared<storage::GroupkeyIndex<R>>(in, column);
  }
};

namespace {
  auto _ = QueryParser::registerPlanOperation<CreateGroupkeyIndex>("CreateGroupkeyIndex");
}

CreateGroupkeyIndex::~CreateGroupkeyIndex() {
}

void CreateGroupkeyIndex::executePlanOperation() {
  const auto &in = input.getTable(0);
  std::shared_ptr<storage::AbstractIndex> _index;
  auto column = _field_definition[0];

  CreateGroupkeyIndexFunctor fun(in, column);
  storage::type_switch<hyrise_basic_types> ts;
  _index = ts(in->typeOfColumn(column), fun);

  io::StorageManager *sm = io::StorageManager::getInstance();
  sm->addInvertedIndex(_index_name, _index);
}

std::shared_ptr<PlanOperation> CreateGroupkeyIndex::parse(const Json::Value &data) {
  auto i = BasicParser<CreateGroupkeyIndex>::parse(data);
  i->setIndexName(data["index_name"].asString());
  return i;
}

void CreateGroupkeyIndex::setIndexName(const std::string &t) {
  _index_name = t;
}

}
}
