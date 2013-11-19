// Copyright (c) 2012 Hasso-Plattner-Institut fuer Softwaresystemtechnik GmbH. All rights reserved.
#ifndef SRC_LIB_STORAGE_GROUPKEYINDEX_H_
#define SRC_LIB_STORAGE_GROUPKEYINDEX_H_

#include <vector>
#include <map>
#include <stdexcept>
#include <algorithm>

#include "helper/types.h"

#include "storage/storage_types.h"
#include "storage/AbstractIndex.h"
#include "storage/AbstractTable.h"

#include <memory>

template<typename T>
class GroupkeyIndex : public AbstractIndex {
private:
  typedef std::map<T, pos_list_t> inverted_index_mutable_t;
  typedef std::map<T, PositionRange> groupkey_offsets_t;
  typedef pos_list_t groupkey_postings_t;

  groupkey_offsets_t _offsets;
  groupkey_postings_t _postings;

public:
  virtual ~GroupkeyIndex() {};

  void shrink() { }

  explicit GroupkeyIndex(const hyrise::storage::c_atable_ptr_t& in, field_t column) {
    if (in != nullptr) {

      // create mutable index
      inverted_index_mutable_t _index;
      for (size_t row = 0; row < in->size(); ++row) {
        T tmp = in->getValue<T>(column, row);
        typename inverted_index_mutable_t::iterator find = _index.find(tmp);
        if (find == _index.end()) {
          pos_list_t pos;
          pos.push_back(row);
          _index[tmp] = pos;
        } else {
          find->second.push_back(row);
        }
      }

      // create readonly index
      _postings.reserve(in->size());
      for (auto it : _index) {
        // copy positions 
        std::copy(it.second.begin(), it.second.end(), std::back_inserter(_postings));
        // set offsets
        auto offset_begin = _postings.end() - it.second.end() + it.second.begin();
        auto offset_end = _postings.end();
        _offsets[it.first] = PositionRange(offset_begin, offset_end, true);
      }
    }
  };


  /**
   * returns a list of positions where key was found.
   */
  PositionRange getPositionsForKey(T key) {
   auto it = _offsets.find(key);
    if (it != _offsets.end()) {
      return PositionRange(it->second.begin, it->second.end, true);
    } else {
      // empty result
      return PositionRange(_postings.begin(), _postings.begin(), true);
    }
  };

  PositionRange getPositionsForKeyLT(T key) {
    auto it = _offsets.lower_bound(key);
    if (it != _offsets.end()) {
      return PositionRange(_postings.begin(), it->second.begin, false);
    } else {
      // all
      return PositionRange(_postings.begin(), _postings.end(), false);
    }
  };

  PositionRange getPositionsForKeyLTE(T key) {
    auto it = _offsets.upper_bound(key);
    if (it != _offsets.end()) {
      return PositionRange(_postings.begin(), it->second.begin, false);
    } else {
      // all
      return PositionRange(_postings.begin(), _postings.end(), false);
    }
  };

  PositionRange getPositionsForKeyGT(T key) {
    auto it = _offsets.upper_bound(key);
    if (it != _offsets.end()) {
      return PositionRange(it->second.begin, _postings.end(), false);
    } else {
      // empty
      return PositionRange(_postings.end(), _postings.end(), false);
    }
  };

  PositionRange getPositionsForKeyGTE(T key) {
    auto it = _offsets.lower_bound(key);
    if (it != _offsets.end()) {
      return PositionRange(it->second.begin, _postings.end(), false);
    } else {
      // empty
      return PositionRange(_postings.end(), _postings.end(), false);
    }
  };

  // PositionRange getPositionsForKeyBetween(T a, T b) {
  //   auto it1 = _offsets.lower_bound(a);
  //   auto it2 = _offsets.upper_bound(b);
  //   return PositionRange(it1->second.begin, it2->second.end, false);
  // };
};
#endif  // SRC_LIB_STORAGE_GROUPKEYINDEX_H_
