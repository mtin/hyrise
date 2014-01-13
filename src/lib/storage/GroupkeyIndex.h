// Copyright (c) 2012 Hasso-Plattner-Institut fuer Softwaresystemtechnik GmbH. All rights reserved.
#pragma once

#include <vector>
#include <map>
#include <stdexcept>
#include <algorithm>

#include "helper/types.h"

#include "storage/storage_types.h"
#include "storage/AbstractIndex.h"
#include "storage/AbstractTable.h"
#include "storage/OrderPreservingDictionary.h"

#include <memory>

namespace hyrise {
namespace storage {

// An inverted index for the main that is created once
// on the main, manually or during a merge.
// The index is readonly.

template<typename T>
class GroupkeyIndex : public AbstractIndex {
private:
  typedef std::map<T, pos_list_t> inverted_index_mutable_t;
  typedef OrderPreservingDictionary<T> dict_t;
  typedef std::shared_ptr<dict_t> dict_ptr_t;
  typedef std::vector<pos_t> groupkey_offsets_t;
  typedef std::vector<pos_t> groupkey_postings_t;
  
  groupkey_offsets_t _offsets;
  groupkey_postings_t _postings;
  dict_ptr_t _dictionary;

public:
  virtual ~GroupkeyIndex() {};

  void shrink() {
    throw std::runtime_error("Shrink not supported for GroupkeyIndex");
  }

  void write_lock() {}

  void read_lock() {}

  void unlock() {}

  explicit GroupkeyIndex(const c_atable_ptr_t& in, field_t column) {
    if (in != nullptr) {
      // save pointer to dictionary
      _dictionary = std::static_pointer_cast<dict_t>(in->dictionaryAt(column));

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
      _offsets.resize(_dictionary->size() + 1);
      _postings.reserve(in->size());

      for (auto &it : _index) {
        // set offset
        auto value_id = _dictionary->getValueIdForValue(it.first);
        _offsets[value_id] = _postings.size();

        // copy positions into postings
        std::copy(it.second.begin(), it.second.end(), std::back_inserter(_postings));
      }

      // set last offset
      _offsets[_dictionary->size()] = _postings.size();
    }
  };


  /**
   * returns a list of positions where key was found.
   */
  PositionRange getPositionsForKey(T key) {
    // TODO: optimize to avoid searching the dictionary twice
    if (_dictionary->valueExists(key)) {
      auto value_id = _dictionary->getValueIdForValue(key);
      auto start = _offsets[value_id];
      auto end = _offsets[value_id+1];
      return PositionRange(_postings.begin()+start, _postings.begin()+end, true); 
    } else {
      // empty result
      return PositionRange(_postings.begin(), _postings.begin(), true);
    }
  };

  PositionRange getPositionsForKeyLT(T key) {
    auto value_id = _dictionary->getLowerBoundValueIdForValue(key);
    const bool value_exists = value_id < _dictionary->size();

    if (value_exists) {
      auto end = _offsets[value_id];
      return PositionRange(_postings.begin(), _postings.begin()+end, false);
    } else {
      // all
      return PositionRange(_postings.begin(), _postings.end(), false);
    }
  };

  PositionRange getPositionsForKeyLTE(T key) {
    auto value_id = _dictionary->getUpperBoundValueIdForValue(key);
    const bool value_exists = value_id < _dictionary->size();

    if (value_exists) {
      auto end = _offsets[value_id];
      return PositionRange(_postings.begin(), _postings.begin()+end, false);
    } else {
      // all
      return PositionRange(_postings.begin(), _postings.end(), false);
    }
  };

  PositionRange getPositionsForKeyGT(T key) {
    auto value_id = _dictionary->getUpperBoundValueIdForValue(key);
    const bool value_exists = value_id < _dictionary->size();

    if (value_exists) {
      auto start = _offsets[value_id];
      return PositionRange(_postings.begin()+start, _postings.end(), false);
    } else {
      // empty
      return PositionRange(_postings.end(), _postings.end(), false);
    }
  };

  PositionRange getPositionsForKeyGTE(T key) {
    auto value_id = _dictionary->getLowerBoundValueIdForValue(key);
    const bool value_exists = value_id < _dictionary->size();

    if (value_exists) {
      auto start = _offsets[value_id];
      return PositionRange(_postings.begin()+start, _postings.end(), false);
    } else {
      // empty
      return PositionRange(_postings.end(), _postings.end(), false);
    }
  };

  // returns range ]a,b[
  PositionRange getPositionsForKeyBetween(T a, T b) {
    auto value_id_a = _dictionary->getUpperBoundValueIdForValue(a);
    auto value_id_b = _dictionary->getLowerBoundValueIdForValue(b);
    const bool value_a_exists = value_id_a < _dictionary->size();
    const bool value_b_exists = value_id_b < _dictionary->size();
    
    if (value_a_exists && value_b_exists) {
      auto start = _offsets[value_id_a];
      auto end = _offsets[value_id_b];
      return PositionRange(_postings.begin()+start, _postings.begin()+end, false);
    } else if (value_a_exists) {
      auto start = _offsets[value_id_a];
      return PositionRange(_postings.begin()+start, _postings.end(), false);
    } else if (value_b_exists) {
      auto end = _offsets[value_id_b];
      return PositionRange(_postings.begin(), _postings.begin()+end, false);
    } else {
      // empty
      return PositionRange(_postings.end(), _postings.end(), false);
    }
  };

};

}
}
