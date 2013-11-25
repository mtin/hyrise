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

  /**
   * add new pair of value and pos to index
   */
  void add(T value, pos_t pos) {
    pthread_rwlock_wrlock(&_rw_lock);
    typename inverted_index_t::iterator find = _index.find(value);
    if (find == _index.end()) {
      pos_list_t *poslist = new pos_list_t;
      poslist->push_back(pos);
      _index.insert(std::make_pair(value, *poslist) );
      // _index[value] = poslist;
    } else {
      find->second.push_back(pos);
    }
    pthread_rwlock_unlock(&_rw_lock);
  };



  PositionRange getPositionsForKey(T key) {
    pthread_rwlock_rdlock(&_rw_lock);
    typename inverted_index_t::iterator it = _index.find(key);
    pthread_rwlock_unlock(&_rw_lock);
    if (it != _index.end()) {
      return PositionRange(it->second.begin(), it->second.end(), true);
    } else {
      // empty result
      return PositionRange(_empty.begin(), _empty.end(), true);
    }
  };

  PositionRange getPositionsForKeyLT(T key) {
    pthread_rwlock_rdlock(&_rw_lock);
    auto r = getPositionsBetween(_index.cbegin(), _index.lower_bound(key));
    pthread_rwlock_unlock(&_rw_lock);
    return r;
  };

  PositionRange getPositionsForKeyLTE(T key) {
    pthread_rwlock_rdlock(&_rw_lock);
    auto r = getPositionsBetween(_index.cbegin(), _index.upper_bound(key));
    pthread_rwlock_unlock(&_rw_lock);
    return r;
  };

  PositionRange getPositionsForKeyBetween(T a, T b) {
    // return range ]a,b[
    pthread_rwlock_rdlock(&_rw_lock);
    auto r = getPositionsBetween(_index.lower_bound(a), _index.lower_bound(b));
    pthread_rwlock_unlock(&_rw_lock);
    return r;
  };

  PositionRange getPositionsForKeyGT(T key) {
    pthread_rwlock_rdlock(&_rw_lock);
    auto r = getPositionsBetween(_index.upper_bound(key), _index.cend());
    pthread_rwlock_unlock(&_rw_lock);
    return r;
  };

  PositionRange getPositionsForKeyGTE(T key) {
    pthread_rwlock_rdlock(&_rw_lock);
    auto r = getPositionsBetween(_index.lower_bound(key), _index.cend());
    pthread_rwlock_unlock(&_rw_lock);
    return r;
  };

};
#endif  // SRC_LIB_STORAGE_DELTAINDEX_H_
