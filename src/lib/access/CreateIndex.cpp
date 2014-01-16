// Copyright (c) 2012 Hasso-Plattner-Institut fuer Softwaresystemtechnik GmbH. All rights reserved.
#include "access/CreateIndex.h"

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
#include "storage/InvertedIndex.h"
#include "storage/PagedIndex.h"

namespace hyrise {
namespace access {

struct CreateIndexFunctor {
  typedef std::shared_ptr<hyrise::storage::AbstractIndex> value_type;
  const storage::c_atable_ptr_t& in;
  size_t column;

  CreateIndexFunctor(const storage::c_atable_ptr_t& t, size_t c):
    in(t), column(c) {}

  template<typename R>
  value_type operator()() {
    return std::make_shared<InvertedIndex<R>>(in, column);
  }
};

struct CreatePagedIndexFunctor {
  typedef std::shared_ptr<hyrise::storage::AbstractIndex> value_type;
  const storage::c_atable_ptr_t& in;
  size_t column;
  size_t _pageSize;

  CreatePagedIndexFunctor(const storage::c_atable_ptr_t& t, size_t c, size_t pageSize):
    in(t), column(c), _pageSize(pageSize) {}

  template<typename R>
  value_type operator()() {
    if (_pageSize>0)
      return std::make_shared<hyrise::storage::PagedIndex<R>>(in, column, _pageSize, /*debug*/false);
    return std::make_shared<hyrise::storage::PagedIndex<R>>(in, column);
  }
};

namespace {
  auto _ = QueryParser::registerPlanOperation<CreateIndex>("CreateIndex");
}

CreateIndex::~CreateIndex() {
}

void CreateIndex::executePlanOperation() {
  const auto &in = input.getTable(0);
  std::shared_ptr<hyrise::storage::AbstractIndex> _index;
  auto column = _field_definition[0];

  storage::type_switch<hyrise_basic_types> ts;

  if (_index_type == "paged") {
    CreatePagedIndexFunctor fun(in, column, _index_paged_page_size);
    _index = ts(in->typeOfColumn(column), fun);
  } else { // if (_index_type == "inverted")
    CreateIndexFunctor fun(in, column);
    _index = ts(in->typeOfColumn(column), fun);
  }

  StorageManager *sm = StorageManager::getInstance();
  sm->addInvertedIndex(_index_name, _index);
}

std::shared_ptr<PlanOperation> CreateIndex::parse(const Json::Value &data) {
  auto i = BasicParser<CreateIndex>::parse(data);
  i->setIndexName(data["index_name"].asString());

  if (data.isMember("index_type"))
    i->setIndexType(data["index_type"].asString());

  if (data.isMember("page_size"))
    i->setIndexPageSize(data["page_size"].asInt());
  else
    i->setIndexPageSize(-1);

  return i;
}

void CreateIndex::setIndexName(const std::string &t) {
  _index_name = t;
}

void CreateIndex::setIndexType(const std::string &t) {
  _index_type = t;
}

void CreateIndex::setIndexPageSize(size_t pageSize) {
  _index_paged_page_size = pageSize;
}

}
}
