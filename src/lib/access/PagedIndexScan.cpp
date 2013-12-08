// Copyright (c) 2012 Hasso-Plattner-Institut fuer Softwaresystemtechnik GmbH. All rights reserved.
#include "access/PagedIndexScan.h"

#include <memory>

#include "access/system/BasicParser.h"
#include "access/json_converters.h"
#include "access/system/QueryParser.h"

#include "io/StorageManager.h"

#include "storage/PagedIndex.h"
#include "storage/meta_storage.h"
#include "storage/PointerCalculator.h"

#include <chrono>

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

  std::shared_ptr<AbstractIndex> _index;
  AbstractIndexValue *_indexValue;

  field_t _column;
  const hyrise::storage::c_atable_ptr_t _inputTable;

  ScanPagedIndexFunctor(AbstractIndexValue *i, std::shared_ptr<AbstractIndex> d, field_t column, const hyrise::storage::c_atable_ptr_t inputTable):
    _index(d), _indexValue(i), _column(column), _inputTable(inputTable) {}

  template<typename ValueType>
  storage::pos_list_t * operator()() {
    auto idx = std::dynamic_pointer_cast<PagedIndex<ValueType>>(_index);
    auto v = static_cast<IndexValue<ValueType>*>(_indexValue);

    storage::pos_list_t intermediate_result;
    std::vector<bool> pages = idx->getPagesForKey(v->value);
    size_t pageSize = idx->getPageSize();

    for (size_t i=0; i<pages.size(); ++i) {
      if (pages[i] != 1)
        continue;

      size_t offsetStart = i * pageSize;
      size_t offsetEnd = std::min<size_t>(offsetStart + pageSize, _inputTable->size());
      for (size_t p=offsetStart; p<offsetEnd; ++p) {
        if (_inputTable->getValue<ValueType>(_column, p) != v->value)
          continue;

        intermediate_result.push_back(p);
      }
    }

        std::cout << std::endl;

    // whacky
    // storage::pos_list_t::iterator iter = result->begin();
    // while (iter != result->end()) {
    //       if(_inputTable->getValue<ValueType>(_column, *iter/*row*/) != v->value) {
    //         iter = result->erase(iter);
    //       }
    //       else
    //         ++iter;
    // }

    // wtf
    storage::pos_list_t *result = new storage::pos_list_t(intermediate_result);
    return result;
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

  StorageManager *sm = StorageManager::getInstance();
  auto idx = sm->getInvertedIndex(_indexName);

  // Handle type of index and value
  storage::type_switch<hyrise_basic_types> ts;
  ScanPagedIndexFunctor fun(_value, idx, _field_definition[0], getInputTable());
  storage::pos_list_t *pos = ts(input.getTable(0)->typeOfColumn(_field_definition[0]), fun);

  addResult(PointerCalculator::create(input.getTable(0), pos));

  auto end_time = std::chrono::high_resolution_clock::now();
  std::cout << std::chrono::duration_cast<std::chrono::seconds>(end_time - start_time).count() << "s " << 
               std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time).count() % 1000 << "ms " << 
               std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time).count() % 1000 << "us " << std::endl;
   
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
