// Copyright (c) 2012 Hasso-Plattner-Institut fuer Softwaresystemtechnik GmbH. All rights reserved.
#ifndef SRC_LIB_STORAGE_DELTAINDEX_H_
#define SRC_LIB_STORAGE_DELTAINDEX_H_

#include <vector>
#include <map>
#include <stdexcept>
#include <algorithm>
#include <memory>

#include "tbb/concurrent_vector.h"
#include "tbb/concurrent_hash_map.h"

#include "helper/types.h"

#include "storage/storage_types.h"
#include "storage/AbstractIndex.h"
#include "storage/AbstractTable.h"

// An inverted index for the delta that can be queried and
// is modifiable to add new values. However, the structure
// is not threadsafe and needs to be synchronized via
// read_lock and write_lock.
// Individual pos_lists for one key are stored sorted.
template<typename T>
class DeltaIndex : public AbstractIndex {
private:

  typedef std::multimap<T, pos_t> inverted_index_t;
  typedef std::pair<typename inverted_index_t::const_iterator, typename inverted_index_t::const_iterator> range_t;
  inverted_index_t _index;

  pthread_rwlock_t _rw_lock;
  pos_list_t _empty;

  const hyrise::storage::c_atable_ptr_t& _table;
  const field_t _column;

public:
  virtual ~DeltaIndex() {
    pthread_rwlock_destroy(&_rw_lock);
  };

  void shrink() {
    throw std::runtime_error("Shrink not supported for DeltaIndex");
  }

  explicit DeltaIndex(const hyrise::storage::c_atable_ptr_t& in, field_t column) : _table(in), _column(column) {
    pthread_rwlock_init(&_rw_lock, NULL);
  };

  void write_lock() {
    if (pthread_rwlock_wrlock(&_rw_lock) != 0)
      std::cout << "ERROR while write locking delta index" <<std::endl;
  }
  
  void read_lock() {
    if (pthread_rwlock_rdlock(&_rw_lock) != 0)
      std::cout << "ERROR while read locking delta index" <<std::endl;
  }

  void unlock() {
    if (pthread_rwlock_unlock(&_rw_lock) != 0)
      std::cout << "ERROR while unlocking delta index" <<std::endl;
  }

  /**
   * add new pair of value and pos to index. as it is not guaranteed that
   * positions are inserted in sorted order, we need to make sure that the
   * last element of the respective pos_list is smaller than the inserted
   * position.
   */
  void add(T value, pos_t pos) {
    _index.emplace(value, pos);
  };

  static void copy(typename inverted_index_t::const_iterator first, typename inverted_index_t::const_iterator last, std::shared_ptr<pos_list_t> result) {
    for(auto it = first; it != last; it++) {
      result->push_back(it->second);
    }
  }

  PositionRange getPositionsForKey(T key) {
    std::shared_ptr<pos_list_t> pos_list(new pos_list_t);
    range_t range = _index.equal_range(key);
    copy(range.first, range.second, pos_list);
    return PositionRange(pos_list->cbegin(), pos_list->cend(), false, pos_list);
  };

  PositionRange getPositionsForKeyLT(T key) {
    std::shared_ptr<pos_list_t> pos_list(new pos_list_t);
    copy(_index.begin(), _index.lower_bound(key), pos_list);
    return PositionRange(pos_list->cbegin(), pos_list->cend(), false, pos_list);
  };

  PositionRange getPositionsForKeyLTE(T key) {
    std::shared_ptr<pos_list_t> pos_list(new pos_list_t);
    copy(_index.begin(), _index.upper_bound(key), pos_list);
    return PositionRange(pos_list->cbegin(), pos_list->cend(), false, pos_list);
  };

  PositionRange getPositionsForKeyBetween(T a, T b) {
    // return range ]a,b[
    std::shared_ptr<pos_list_t> pos_list(new pos_list_t);
    copy(_index.lower_bound(a), _index.upper_bound(b), pos_list);
    return PositionRange(pos_list->cbegin(), pos_list->cend(), false, pos_list);
  };

  PositionRange getPositionsForKeyGT(T key) {
    std::shared_ptr<pos_list_t> pos_list(new pos_list_t);
    copy(_index.upper_bound(key), _index.end(), pos_list);
    return PositionRange(pos_list->cbegin(), pos_list->cend(), false, pos_list);
  };

  PositionRange getPositionsForKeyGTE(T key) {
    std::shared_ptr<pos_list_t> pos_list(new pos_list_t);
    copy(_index.lower_bound(key), _index.end(), pos_list);
    return PositionRange(pos_list->cbegin(), pos_list->cend(), false, pos_list);
  };

};
#endif  // SRC_LIB_STORAGE_DELTAINDEX_H_
