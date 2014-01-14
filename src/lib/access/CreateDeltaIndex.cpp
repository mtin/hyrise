// Copyright (c) 2012 Hasso-Plattner-Institut fuer Softwaresystemtechnik GmbH. All rights reserved.
#include "access/CreateDeltaIndex.h"

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
#include "storage/DeltaIndex.h"
#include "storage/Store.h"

namespace hyrise {
namespace access {

struct CreateDeltaIndexFunctor {
  typedef std::shared_ptr<storage::AbstractIndex> value_type;
  const storage::c_atable_ptr_t& in;
  size_t column;

  CreateDeltaIndexFunctor(const storage::c_atable_ptr_t& t, size_t c):
    in(t), column(c) {}

  template<typename R>
  value_type operator()() {
    return std::make_shared<storage::DeltaIndex<R>>(in, column);
  }
};

namespace {
  auto _ = QueryParser::registerPlanOperation<CreateDeltaIndex>("CreateDeltaIndex");
}

CreateDeltaIndex::~CreateDeltaIndex() {
}

void CreateDeltaIndex::executePlanOperation() {
  const auto &in = input.getTable(0);

  auto t = input.getTables()[0];
  auto c_store = std::dynamic_pointer_cast<const storage::Store>(t);
  auto store = std::const_pointer_cast<storage::Store>(c_store);

  // we only work on stores
  if (!store)
    throw std::runtime_error("IndexAwareTableScan only works on stores");

  std::shared_ptr<storage::AbstractIndex> _index;
  auto column = _field_definition[0];

  CreateDeltaIndexFunctor fun(in, column);
  storage::type_switch<hyrise_basic_types> ts;
  _index = ts(in->typeOfColumn(column), fun);

  io::StorageManager *sm = io::StorageManager::getInstance();
  sm->addInvertedIndex(_index_name, _index);
  store->addDeltaIndex(_index, column);
}

std::shared_ptr<PlanOperation> CreateDeltaIndex::parse(const Json::Value &data) {
  auto i = BasicParser<CreateDeltaIndex>::parse(data);
  i->setIndexName(data["index_name"].asString());
  return i;
}

void CreateDeltaIndex::setIndexName(const std::string &t) {
  _index_name = t;
}

}
}
