// Copyright (c) 2012 Hasso-Plattner-Institut fuer Softwaresystemtechnik GmbH. All rights reserved.
#pragma once

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

namespace hyrise {
namespace storage {

// An inverted index for the delta that can be queried and
// is modifiable to add new values. However, the structure
// is not threadsafe and needs to be synchronized via
// read_lock and write_lock.
// Individual pos_lists for one key are stored sorted.
template<typename T>
class DeltaIndex : public AbstractIndex {
private:

  typedef std::map<T, pos_list_t> inverted_index_t;
  inverted_index_t _index;

  pthread_rwlock_t _rw_lock;
  pos_list_t _empty;

  const hyrise::storage::c_atable_ptr_t& _table;
  const field_t _column;

  PositionRange getPositionsBetween(typename inverted_index_t::const_iterator begin, const typename inverted_index_t::const_iterator end) {
    PositionRange positionRange;
    auto it = begin;
    while(it != end) {
      positionRange.add(it->second.begin(), it->second.end());
      it++;
    }
    return positionRange;
  }

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
    typename inverted_index_t::iterator find = _index.find(value);
    if (find == _index.end()) {
      pos_list_t *poslist = new pos_list_t;
      poslist->push_back(pos);
      _index.insert(std::make_pair(value, *poslist) );
    } else {
      // find position to insert...
      auto it = find->second.end();
      --it;
      while (*it > pos) --it;
      ++it;
      // ... and insert
      find->second.insert(it, pos);
    }
  };



  PositionRange getPositionsForKey(T key) {
    typename inverted_index_t::iterator it = _index.find(key);
    if (it != _index.end()) {
      return PositionRange(it->second.begin(), it->second.end(), true);
    } else {
      // empty result
      return PositionRange(_empty.begin(), _empty.end(), true);
    }
  };

  PositionRange getPositionsForKeyLT(T key) {
    return getPositionsBetween(_index.cbegin(), _index.lower_bound(key));
  };

  PositionRange getPositionsForKeyLTE(T key) {
    return getPositionsBetween(_index.cbegin(), _index.upper_bound(key));
  };

  PositionRange getPositionsForKeyBetween(T a, T b) {
    // return range ]a,b[
    return getPositionsBetween(_index.lower_bound(a), _index.upper_bound(b));
  };

  PositionRange getPositionsForKeyGT(T key) {
    return getPositionsBetween(_index.upper_bound(key), _index.cend());
  };

  PositionRange getPositionsForKeyGTE(T key) {
    return getPositionsBetween(_index.lower_bound(key), _index.cend());
  };

};

}
}
