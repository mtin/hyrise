//// Copyright (c) 2012 Hasso-Plattner-Institut fuer Softwaresystemtechnik GmbH. All rights reserved.
#include "access/PagedIndexScan.h"

#include <memory>
#include <iostream>

#include "access/system/BasicParser.h"
#include "access/json_converters.h"
#include "access/system/QueryParser.h"

#include "io/StorageManager.h"

#include "storage/PagedIndex.h"
#include "storage/meta_storage.h"
#include "storage/PointerCalculator.h"

#include <chrono>
#include <boost/dynamic_bitset.hpp>

namespace hyrise {
namespace access {

struct CreateIndexValueFunctor {
  typedef AbstractIndexValue *value_type;

  const Json::Value &_d;

  explicit CreateIndexValueFunctor(const Json::Value &c): _d(c) {}

  template<typename R>
  value_type operator()() {
    IndexValue<R> *v = new IndexValue<R>();
    v->value = json_converter::convert<R>(_d["value"]);
    return v;
  }
};

struct ScanPagedIndexFunctor {
  typedef storage::pos_list_t *value_type;

  std::shared_ptr<hyrise::storage::AbstractIndex> _index;
  AbstractIndexValue *_indexValue;

  field_t _column;
  const hyrise::storage::c_atable_ptr_t _inputTable;

  ScanPagedIndexFunctor(AbstractIndexValue *i, std::shared_ptr<hyrise::storage::AbstractIndex> d, field_t column, const hyrise::storage::c_atable_ptr_t inputTable):
    _index(d), _indexValue(i), _column(column), _inputTable(inputTable) {}

  template<typename ValueType>
  storage::pos_list_t * operator()() {
    auto idx = std::dynamic_pointer_cast<hyrise::storage::PagedIndex>(_index);
    auto v = static_cast<IndexValue<ValueType>*>(_indexValue);

    return idx->getPositionsForKey(v->value, _column, _inputTable);
  }
};


namespace {
  auto _ = QueryParser::registerPlanOperation<PagedIndexScan>("PagedIndexScan");
}

PagedIndexScan::~PagedIndexScan() {
  delete _value;
}

void PagedIndexScan::executePlanOperation() {
  auto start_time = std::chrono::high_resolution_clock::now();

  hyrise::io::StorageManager *sm = hyrise::io::StorageManager::getInstance();
  auto idx = sm->getInvertedIndex(_indexName);

  // Handle type of index and value
  storage::type_switch<hyrise_basic_types> ts;
  ScanPagedIndexFunctor fun(_value, idx, _field_definition[0], getInputTable());
  storage::pos_list_t *pos = ts(input.getTable(0)->typeOfColumn(_field_definition[0]), fun);

  addResult(hyrise::storage::PointerCalculator::create(input.getTable(0), pos));

  auto end_time = std::chrono::high_resolution_clock::now();
  std::cout << std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time).count() << " us (PagedIndexScan)" << std::endl;
   
}

std::shared_ptr<PlanOperation> PagedIndexScan::parse(const Json::Value &data) {
  auto s = BasicParser<PagedIndexScan>::parse(data);
  storage::type_switch<hyrise_basic_types> ts;
  CreateIndexValueFunctor civf(data);
  s->_value = ts(data["vtype"].asUInt(), civf);
  s->_indexName = data["index"].asString();
  return s;
}

const std::string PagedIndexScan::vname() {
  return "PagedIndexScan";
}

void PagedIndexScan::setIndexName(const std::string &name) {
  _indexName = name;
}

}
}
