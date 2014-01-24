// Copyright (c) 2012 Hasso-Plattner-Institut fuer Softwaresystemtechnik GmbH. All rights reserved.
#include "access/MergeStoreIndexAware.h"

#include "access/system/QueryParser.h"

#include "io/StorageManager.h"
#include "storage/PagedIndex.h"
#include "storage/DeltaIndex.h"

#include "helper/checked_cast.h"
#include "storage/Store.h"

namespace hyrise {
namespace access {


namespace {
  auto _ = QueryParser::registerPlanOperation<MergeStoreIndexAware>("MergeStoreIndexAware");
}

MergeStoreIndexAware::~MergeStoreIndexAware() {
}

void MergeStoreIndexAware::executePlanOperation() {
  auto t = checked_pointer_cast<const storage::Store>(getInputTable());
  auto store = std::const_pointer_cast<storage::Store>(t);
  store->merge();

  addResult(store);

  // get paged index
  hyrise::io::StorageManager *sm = hyrise::io::StorageManager::getInstance();
  auto idx = sm->getInvertedIndex(_index_name);
  auto pagedIdx = std::dynamic_pointer_cast<hyrise::storage::PagedIndex>(idx);
  idx = sm->getInvertedIndex(_delta_name);
  //auto deltaIdx = std::dynamic_pointer_cast<hyrise::storage::DeltaIndex>(idx);

  // rebuild index
  //pagedIdx->rebuildIndex(store->getMainTable());

  // rebuild index with delta
  //pagedIdx->rebuildIndexWithDelta(store->getMainTable(), );
}

std::shared_ptr<PlanOperation> MergeStoreIndexAware::parse(const Json::Value& data) {
  auto i = BasicParser<MergeStoreIndexAware>::parse(data);
  i->setIndexName(data["index_name"].asString());
  i->setDeltaName(data["delta_index_name"].asString());

  return i;
}

void MergeStoreIndexAware::setIndexName(const std::string &t) {
  _index_name = t;
}

void MergeStoreIndexAware::setDeltaName(const std::string &t) {
  _delta_name = t;
}



namespace {
  auto _2 = QueryParser::registerPlanOperation<MergeStoreIndexAwareBaseline>("MergeStoreIndexAwareBaseline");
}

MergeStoreIndexAwareBaseline::~MergeStoreIndexAwareBaseline() {
}

void MergeStoreIndexAwareBaseline::executePlanOperation() {
  std::vector<storage::c_atable_ptr_t> tables;
  // Add all tables to the game
  for (auto& table: input.getTables()) {
    if (auto store = std::dynamic_pointer_cast<const storage::Store>(table)) {
      tables.push_back(store->getMainTable());
      tables.push_back(store->getDeltaTable());
    } else {
      tables.push_back(table);
    }
  }

  // Call the Merge
  storage::TableMerger merger(new storage::DefaultMergeStrategy(), new storage::SequentialHeapMerger());
  auto new_table = input.getTable(0)->copy_structure();

  // Switch the tables
  auto merged_tables = merger.mergeToTable(new_table, tables);
  const auto &result = std::make_shared<storage::Store>(new_table);

  output.add(result);

  // get index
  hyrise::io::StorageManager *sm = hyrise::io::StorageManager::getInstance();
  auto idx = sm->getInvertedIndex(_index_name);
  auto pagedIdx = std::dynamic_pointer_cast<hyrise::storage::PagedIndex>(idx);

  // rebuild index
  pagedIdx->rebuildIndex(result);
}

std::shared_ptr<PlanOperation> MergeStoreIndexAwareBaseline::parse(const Json::Value& data) {
  auto i = BasicParser<MergeStoreIndexAwareBaseline>::parse(data);
  i->setIndexName(data["index_name"].asString());

  return i;
}

void MergeStoreIndexAwareBaseline::setIndexName(const std::string &t) {
  _index_name = t;
}

}
}
