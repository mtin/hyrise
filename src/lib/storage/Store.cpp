// Copyright (c) 2012 Hasso-Plattner-Institut fuer Softwaresystemtechnik GmbH. All rights reserved.
#include <storage/Store.h>
#include <iostream>


#include <io/TransactionManager.h>
#include <storage/storage_types.h>
#include <storage/PrettyPrinter.h>

#include <helper/vector_helpers.h>
#include <helper/locking.h>
#include <helper/cas.h>

namespace hyrise { namespace storage {

TableMerger* createDefaultMerger() {
  return new TableMerger(new DefaultMergeStrategy, new SequentialHeapMerger, false);
}

Store::Store(atable_ptr_t main_table) :
  delta(main_table->copy_structure_modifiable()),
  merger(createDefaultMerger()),
  _cidBeginVector(main_table->size(), 0),
  _cidEndVector(main_table->size(), tx::INF_CID),
  _tidVector(main_table->size(), tx::UNKNOWN) {
  //pthread_rwlock_init(&_rw_lock, NULL);
  setUuid();
  main_tables.push_back(main_table);
}

Store::~Store() {
  //pthread_rwlock_destroy(&_rw_lock); 
  delete merger;
}

void Store::merge() {
  if (merger == nullptr) {
    throw std::runtime_error("No Merger set.");
  }

  //pthread_rwlock_wrlock(&_rw_lock);

  // Create new delta and merge
  atable_ptr_t new_delta = delta->copy_structure_modifiable();

  // Prepare the merge
  std::vector<c_atable_ptr_t> tmp(main_tables.begin(), main_tables.end());
  tmp.push_back(delta);

  // get valid positions
  std::vector<bool> validPositions(_cidBeginVector.size());
  tx::transaction_cid_t last_commit_id = tx::TransactionManager::getInstance().getLastCommitId();
  functional::forEachWithIndex(_cidBeginVector, [&](size_t i, bool v){
    validPositions[i] = isVisibleForTransaction(i, last_commit_id, tx::MERGE_TID);
  });

  main_tables = merger->merge(tmp, true, validPositions);

  // Fixup the cid and tid vectors
  _cidBeginVector.assign(main_tables[0]->size(), tx::UNKNOWN_CID);
  _cidEndVector.assign(main_tables[0]->size(), tx::INF_CID);
  _tidVector.assign(main_tables[0]->size(), tx::START_TID);

  // Replace the delta partition
  delta = new_delta;

  // hotfix !!
  size_t num = 100000;
  size_t start = delta->size();
  delta->reserve(start + num);
  auto main_tables_size = functional::sum(main_tables, 0ul, [](atable_ptr_t& t){return t->size();});
  _cidBeginVector.reserve(main_tables_size + start + num);
  _cidEndVector.reserve(main_tables_size + start + num);
  _tidVector.reserve(main_tables_size + start + num);

  //pthread_rwlock_unlock(&_rw_lock);
}


std::vector< atable_ptr_t > Store::getMainTables() const {
  return main_tables;
}

atable_ptr_t Store::getDeltaTable() const {
  return delta;
}

const ColumnMetadata *Store::metadataAt(const size_t column_index, const size_t row_index, const table_id_t table_id) const {
  size_t offset = 0;

  //pthread_rwlock_rdlock(&_rw_lock);
  for (size_t main = 0; main < main_tables.size(); main++)
    if (main_tables[main]->size() + offset > row_index) {
      //pthread_rwlock_unlock(&_rw_lock);
      return main_tables[main]->metadataAt(column_index, row_index - offset, table_id);
    } else {
      offset += main_tables[main]->size();
    }

  // row is not in main tables. return metadata from delta
  auto res = delta->metadataAt(column_index, row_index - offset, table_id);
  //pthread_rwlock_unlock(&_rw_lock);
  return res;
}

void Store::setDictionaryAt(AbstractTable::SharedDictionaryPtr dict, const size_t column, const size_t row, const table_id_t table_id) {
  size_t offset = 0;

  //pthread_rwlock_rdlock(&_rw_lock);
  for (size_t main = 0; main < main_tables.size(); main++)
    if (main_tables[main]->size() + offset > row) {
      main_tables[main]->setDictionaryAt(dict, column, row - offset, table_id);
      //pthread_rwlock_unlock(&_rw_lock);
      return;
    } else {
      offset += main_tables[main]->size();
    }

  // row is not in main tables. set dict in delta
  delta->setDictionaryAt(dict, column, row - offset, table_id);
  //pthread_rwlock_unlock(&_rw_lock);
}

const AbstractTable::SharedDictionaryPtr& Store::dictionaryAt(const size_t column, const size_t row, const table_id_t table_id) const {
  // if (!row) {
  //   return this->dictionaryByTableId(column, table_id);
  // }

  // if (table_id) {
  //   if (table_id < main_tables.size()) {
  //     return main_tables[table_id]->dictionaryAt(column, row);
  //   } else {
  //     return delta->dictionaryAt(column, row);
  //   }
  // }

  size_t offset = 0;

  //pthread_rwlock_rdlock(&_rw_lock);
  for (size_t main = 0; main < main_tables.size(); main++)
    if (main_tables[main]->size() + offset > row) {
      //pthread_rwlock_unlock(&_rw_lock);
      return main_tables[main]->dictionaryAt(column, row - offset);
    } else {
      offset += main_tables[main]->size();
    }

  //pthread_rwlock_unlock(&_rw_lock);
  // row is not in main tables. return row from delta
  return delta->dictionaryAt(column, row - offset);
}

const AbstractTable::SharedDictionaryPtr& Store::dictionaryByTableId(const size_t column, const table_id_t table_id) const {
  assert(table_id <= main_tables.size());

  if (table_id < main_tables.size()) {
    //pthread_rwlock_unlock(&_rw_lock);
    return main_tables[table_id]->dictionaryByTableId(column, table_id);
  } else {
    //pthread_rwlock_unlock(&_rw_lock);
    return delta->dictionaryByTableId(column, table_id);
  }
}

inline Store::table_offset_idx_t Store::responsibleTable(const size_t row) const {
  size_t offset = 0;
  size_t tableIdx = 0;

  //pthread_rwlock_rdlock(&_rw_lock);
  for (const auto& table: main_tables) {
    size_t sz = table->size();
    if ((sz + offset) > row) {
      //pthread_rwlock_unlock(&_rw_lock);
      return {table, row - offset, tableIdx};
    } else {
      offset += sz;
    }
    ++tableIdx;
  }

  if ((delta->size() + offset) > row) {
    //pthread_rwlock_unlock(&_rw_lock);
    return {delta, row - offset, tableIdx};
  }
  //pthread_rwlock_unlock(&_rw_lock);
  throw std::out_of_range("Requested row is located beyond store boundaries");
}

void Store::setValueId(const size_t column, const size_t row, ValueId vid) {
  //pthread_rwlock_rdlock(&_rw_lock);
  auto location = responsibleTable(row);
  location.table->setValueId(column, location.offset_in_table, vid);
  //pthread_rwlock_unlock(&_rw_lock);
}

ValueId Store::getValueId(const size_t column, const size_t row) const {
  //pthread_rwlock_rdlock(&_rw_lock);
  auto location = responsibleTable(row);
  ValueId valueId = location.table->getValueId(column, location.offset_in_table);
  valueId.table = location.table_index;
  //pthread_rwlock_unlock(&_rw_lock);
  return valueId;
}


size_t Store::size() const {
  //pthread_rwlock_rdlock(&_rw_lock);
  size_t main_tables_size = functional::foldLeft(main_tables, 0ul,
    [](size_t s, const atable_ptr_t& t){return s + t->size();});
  auto res = main_tables_size + delta->size();
  //pthread_rwlock_unlock(&_rw_lock);
  return res;
}

size_t Store::deltaOffset() const {
  return size() - delta->size();
}

size_t Store::columnCount() const {
  return delta->columnCount();
}

unsigned Store::partitionCount() const {
  return main_tables[0]->partitionCount();
}

size_t Store::partitionWidth(const size_t slice) const {
  // TODO we now require that all main tables have the same layout
  return main_tables[0]->partitionWidth(slice);
}


void Store::print(const size_t limit) const {
  PrettyPrinter::print(this, std::cout, "Store", limit, 0);
}

void Store::setMerger(TableMerger *_merger) {
  delete merger;
  merger = _merger;
}

void Store::setDelta(atable_ptr_t _delta) {
  //pthread_rwlock_wrlock(&_rw_lock);
  delta = _delta;
  //pthread_rwlock_unlock(&_rw_lock);
}

atable_ptr_t Store::copy() const {
  //pthread_rwlock_wrlock(&_rw_lock);
  std::shared_ptr<Store> new_store = std::make_shared<Store>(main_tables[0]);

  new_store->delta = delta->copy();

  if (merger == nullptr) {
    new_store->merger = nullptr;
  } else {
    new_store->merger = merger->copy();
  }

  //pthread_rwlock_unlock(&_rw_lock);
  return new_store;
}


const attr_vectors_t Store::getAttributeVectors(size_t column) const {
  attr_vectors_t tables;
  for (const auto& main: main_tables) {
    const auto& subtables = main->getAttributeVectors(column);
    tables.insert(tables.end(), subtables.begin(), subtables.end());
  }
  const auto& subtables = delta->getAttributeVectors(column);
  tables.insert(tables.end(), subtables.begin(), subtables.end());
  return tables;
}

void Store::debugStructure(size_t level) const {
  //pthread_rwlock_rdlock(&_rw_lock);
  std::cout << std::string(level, '\t') << "Store " << this << std::endl;
  std::cout << std::string(level, '\t') << "(main) " << this << std::endl;
  for (const auto& m: main_tables) {
    m->debugStructure(level+1);
  }
  std::cout << std::string(level, '\t') << "(delta) " << this << std::endl;
  delta->debugStructure(level+1);
  //pthread_rwlock_unlock(&_rw_lock);
}

bool Store::isVisibleForTransaction(pos_t pos, tx::transaction_cid_t last_commit_id, tx::transaction_id_t tid) const {
  if (_tidVector[pos] == tid) {
    if (last_commit_id >= _cidBeginVector[pos]) {
      // row was inserted and committed by another transaction, then deleted by our transaction
      // if we have a lock for it but someone else committed a delete, something is wrong
      assert(_cidEndVector[pos] == tx::INF_CID);
      return false;
    } else {
      // we inserted this row - nobody should have deleted it yet
      assert(_cidEndVector[pos] == tx::INF_CID);
      return true;
    }
  } else {
    if (last_commit_id >= _cidBeginVector[pos]) {
      // we are looking at a row that was inserted and deleted before we started - we should see it unless it was already deleted again
      if(last_commit_id >= _cidEndVector[pos]) {
        // the row was deleted and the delete was committed before we started our transaction
        return false;
      } else {
        // the row was deleted but the commit happened after we started our transaction (or the delete was not committed yet)
        return true;
      }
    } else {
      // we are looking at a row that was inserted after we started
      assert(_cidEndVector[pos] > last_commit_id);
      return false;
    }
  }
}

// This method iterates of the pos list and validates each position
void Store::validatePositions(pos_list_t& pos, tx::transaction_cid_t last_commit_id, tx::transaction_id_t tid) const {
  // Make sure we captured all rows
  assert(_cidBeginVector.size() == size() && _cidEndVector.size() == size() && _tidVector.size() == size());

  //pthread_rwlock_rdlock(&_rw_lock);

  // Pos is nullptr, we should circumvent
  auto end = std::remove_if(std::begin(pos), std::end(pos), [&](const pos_t& v){

    return !isVisibleForTransaction(v, last_commit_id, tid);

  } );
  if (end != pos.end())
    pos.erase(end, pos.end());
  //pthread_rwlock_unlock(&_rw_lock);
}

pos_list_t Store::buildValidPositions(tx::transaction_cid_t last_commit_id, tx::transaction_id_t tid) const {
  pos_list_t result;
  //pthread_rwlock_rdlock(&_rw_lock);
  functional::forEachWithIndex(_cidBeginVector, [&](size_t i, tx::transaction_cid_t v){
    if(isVisibleForTransaction(i, last_commit_id, tid)) result.push_back(i);
  });
  //pthread_rwlock_unlock(&_rw_lock);
  return std::move(result);
}

std::pair<size_t, size_t> Store::resizeDelta(size_t num) {
  assert(num > delta->size());
  return appendToDelta(num - delta->size());
}

std::pair<size_t, size_t> Store::appendToDelta(size_t num) {
  // pthread_rwlock_wrlock(&_rw_lock);


  size_t start = delta->size();
  std::pair<size_t, size_t> result = {start, start + num};

  // std::cout << "WARNING: Be careful, appendToDelta not threadsafe! Resizing to " << start + num << std::endl;

  // Update Delta
  delta->resize(start + num);
  // Update CID, TID and valid
  auto main_tables_size = functional::sum(main_tables, 0ul, [](atable_ptr_t& t){return t->size();});

  _cidBeginVector.resize(main_tables_size + start + num, tx::INF_CID);
  _cidEndVector.resize(main_tables_size + start + num, tx::INF_CID);
  _tidVector.resize(main_tables_size + start + num, tx::START_TID);

  // pthread_rwlock_unlock(&_rw_lock);

  return std::move(result);
}

void Store::copyRowToDelta(const c_atable_ptr_t& source, const size_t src_row, const size_t dst_row, tx::transaction_id_t tid) {
  //pthread_rwlock_rdlock(&_rw_lock);
  auto main_tables_size = functional::sum(main_tables, 0ul, [](atable_ptr_t& t){return t->size();});

  // Update the validity
  _tidVector[main_tables_size + dst_row] = tid;

  delta->copyRowFrom(source, src_row, dst_row, true);
  //pthread_rwlock_unlock(&_rw_lock);
}

void Store::copyRowToDeltaFromJSONVector(const std::vector<Json::Value>& source, size_t dst_row, tx::transaction_id_t tid) {
  //pthread_rwlock_rdlock(&_rw_lock);
  auto main_tables_size = functional::sum(main_tables, 0ul, [](atable_ptr_t& t){return t->size();});

  // Update the validity
  _tidVector[main_tables_size + dst_row] = tid;

  delta->copyRowFromJSONVector(source, dst_row);
  //pthread_rwlock_unlock(&_rw_lock);
}

void Store::copyRowToDeltaFromStringVector(const std::vector<std::string>& source, size_t dst_row, tx::transaction_id_t tid) {
  //pthread_rwlock_rdlock(&_rw_lock);
  auto main_tables_size = functional::sum(main_tables, 0ul, [](atable_ptr_t& t){return t->size();});

  // Update the validity
  _tidVector[main_tables_size + dst_row] = tid;

  delta->copyRowFromStringVector(source, dst_row);
  //pthread_rwlock_unlock(&_rw_lock);
}

tx::TX_CODE Store::commitPositions(const pos_list_t& pos, const tx::transaction_cid_t cid, bool valid) {
  //pthread_rwlock_rdlock(&_rw_lock);
  for(const auto& p : pos) {
    if(valid) {
      _cidBeginVector[p] = cid;
    } else {
      _cidEndVector[p] = cid;
    }
    _tidVector[p] = tx::START_TID;
  }

  persist_scattered(pos, valid);
  //pthread_rwlock_unlock(&_rw_lock);
  return tx::TX_CODE::TX_OK;
}

tx::TX_CODE Store::checkForConcurrentCommit(const pos_list_t& pos, const tx::transaction_id_t tid) const {
  //pthread_rwlock_rdlock(&_rw_lock);
  for(const auto& p : pos) {
    if (_tidVector[p] != tid) {
      //pthread_rwlock_unlock(&_rw_lock);
      return tx::TX_CODE::TX_FAIL_CONCURRENT_COMMIT;
    }
  }
  //pthread_rwlock_unlock(&_rw_lock);
  return tx::TX_CODE::TX_OK;
}

tx::TX_CODE Store::markForDeletion(const pos_t pos, const tx::transaction_id_t tid) {
  //pthread_rwlock_rdlock(&_rw_lock);
  if(atomic_cas(&_tidVector[pos], tx::START_TID, tid)) {
    //pthread_rwlock_unlock(&_rw_lock);
    return tx::TX_CODE::TX_OK;
  }

  if(_tidVector[pos] == tid) {
    // It is a row that we inserted ourselves. We remove the TID, leaving it with TID=0,begin=0,end=0 which is invisible to everyone
    // No need for a CAS here since we already have it "locked"
    _tidVector[pos] = 0;
    //pthread_rwlock_unlock(&_rw_lock);
    return tx::TX_CODE::TX_OK;
  }

  //pthread_rwlock_unlock(&_rw_lock);
  return tx::TX_CODE::TX_FAIL_CONCURRENT_COMMIT;
}

tx::TX_CODE Store::unmarkForDeletion(const pos_list_t& pos, const tx::transaction_id_t tid) {
  //pthread_rwlock_rdlock(&_rw_lock);
  for(const auto& p : pos) {
    if (_tidVector[p] == tid)
      _tidVector[p] = tx::START_TID;
  }
  //pthread_rwlock_unlock(&_rw_lock);
  return tx::TX_CODE::TX_OK;
}


void Store::persist_scattered(const pos_list_t& elements, bool new_elements) const {
#ifdef PERSISTENCY_NVRAM
  if(new_elements) {
    delta->persist_scattered(elements, true);
    _cidBeginVector.persist_scattered(elements, true);
  } else {
    _cidEndVector.persist_scattered(elements, false);
  }
#endif
}

}}
