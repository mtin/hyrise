// Copyright (c) 2012 Hasso-Plattner-Institut fuer Softwaresystemtechnik GmbH. All rights reserved.
#include <set>

#include "access/InsertScan.h"
#include "access/system/ResponseTask.h"

#include "json_converters.h"

#include "helper/vector_helpers.h"
#include "helper/checked_cast.h"
#include "helper/stringhelpers.h"

#include "io/TransactionManager.h"
#include "io/ResourceManager.h"

#include "storage/Store.h"
#include "storage/Serial.h"
#include "storage/meta_storage.h"

namespace hyrise {
namespace access {

namespace {
  auto _ = QueryParser::registerPlanOperation<InsertScan>("InsertScan");
}



InsertScan::~InsertScan() {
}

storage::atable_ptr_t InsertScan::buildFromJson() {

  auto result = input.getTable()->copy_structure_modifiable();
  result->resize(_raw_data.size());

  set_json_value_functor fun(result);
  storage::type_switch<hyrise_basic_types> ts;

  auto col_count = input.getTable()->columnCount();
  auto tab = input.getTable();

  // Storage for field references
  std::set<field_t> serialFields;

  // Check if table has serial generators defined
  auto& res_man = io::ResourceManager::getInstance();
  for(size_t c=0; c < col_count; ++c) {
    auto serial_name = std::to_string(tab->getUuid()) + "_" + tab->nameOfColumn(c);
    if (res_man.exists(serial_name)) {
      serialFields.insert(c);
    }
  }

  for (size_t r=0, row_count=_raw_data.size(); r < row_count; ++r ) {

    size_t offset = 0;
    for(size_t c=0; c < col_count; ++c) {
      if (serialFields.count(c) != 0) {
        auto serial_name = std::to_string(tab->getUuid()) + "_" + tab->nameOfColumn(c);
        auto k = res_man.get<Serial>(serial_name)->next();
        _generatedKeys->push_back(k);
        result->setValue<hyrise_int_t>(c, r, k);
        ++offset;
      } else {
        fun.set(c,r,_raw_data[r][c-offset]);
        ts(result->typeOfColumn(c), fun);
      }

    }
  }

  return result;

}

void InsertScan::executePlanOperation() {
  const auto& c_store = checked_pointer_cast<const storage::Store>(input.getTable(0));

  // Cast the constness away
  auto store = std::const_pointer_cast<storage::Store>(c_store);

  if (!_data) {
    //_data = buildFromJson(); // nope!

    // ==================
    // TODO: refactor me!
    // ==================
    const size_t beforeSize = store->size();
    const size_t columnCount = store->columnCount();
    const size_t rowCount = _raw_data.size();
    const auto &writeArea = store->appendToDelta(rowCount);
    auto &mods = tx::TransactionManager::getInstance()[_txContext.tid];

    // determine serial [=autoincrement] fields in store
    auto &resMgr = io::ResourceManager::getInstance();
    std::set<field_t> serialFields;
    for(size_t c=0; c<columnCount; ++c) {
      auto serial_name = std::to_string(store->getUuid()) + "_" + store->nameOfColumn(c);
      if (resMgr.exists(serial_name)) {
        serialFields.insert(c);
      }
    }

    // extend _raw_data with serial fields (if any)
    if(!serialFields.empty()) {
      std::vector<std::vector<Json::Value>> extended_raw_data(rowCount, std::vector<Json::Value>(columnCount));
      for(size_t r=0; r<rowCount; ++r) {
        size_t columnOffset = 0;
        for(size_t c=0; c<columnCount; ++c) {
          if(serialFields.count(c) != 0) {
            std::string serialName = std::to_string(store->getUuid()) + "_" + store->nameOfColumn(c);
            Json::Value v(resMgr.get<Serial>(serialName)->next());
            extended_raw_data[r][c] = v;
            ++columnOffset;
          } else {
            extended_raw_data[r][c] = _raw_data[r][c-columnOffset];
          }
        }
      }
      _raw_data = extended_raw_data;
    }

    for(size_t i=0; i<rowCount; ++i) {
      store->copyRowToDeltaFromJSONVector(_raw_data[i], writeArea.first+i, _txContext.tid);
      mods.insertPos(store, beforeSize+i);

      uint64_t bitmask = (1 << (_raw_data[0].size() + 1)) - 1;
      std::vector<ValueId> vids = store->copyValueIds(beforeSize+i);
      io::Logger::getInstance().logValue(mods.tid, reinterpret_cast<uintptr_t>(store.get()), beforeSize+i, 0, bitmask, &vids);
    }

    auto rsp = getResponseTask();
    if (rsp != nullptr)
      rsp->incAffectedRows(_raw_data.size());
    addResult(input.getTable(0));
    return;
  }

  // Delta Table Size
  const auto& beforeSize = store->size();

  auto writeArea = store->appendToDelta(_data->size());

  // Get the modifications record
  auto& mods = tx::TransactionManager::getInstance()[_txContext.tid];
  for(size_t i=0, upper = _data->size(); i < upper; ++i) {
    store->copyRowToDelta(_data, i, writeArea.first+i, _txContext.tid);
    mods.insertPos(store, beforeSize+i);

    uint64_t bitmask = (1 << (_data->columnCount() + 1)) - 1;
    std::vector<ValueId> vids = _data.get()->copyValueIds(i);
    io::Logger::getInstance().logValue(mods.tid, reinterpret_cast<uintptr_t>(store.get()), beforeSize+i, 0, bitmask, &vids);
  }

  auto rsp = getResponseTask();
  if (rsp != nullptr)
    rsp->incAffectedRows(_data->size());

  addResult(input.getTable(0));
}

void InsertScan::setInputData(const storage::atable_ptr_t &c) {
  _data = c;
}

std::shared_ptr<PlanOperation> InsertScan::parse(Json::Value &data) {
  auto result = std::make_shared<InsertScan>();

  if (data.isMember("data")) {
    result->_raw_data = functional::collect(data["data"], [](const Json::Value& v){
      return functional::collect(v, [](const Json::Value& c){ return Json::Value(c); });
    });
  }
  return result;
}

}
}
