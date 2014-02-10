// Copyright (c) 2012 Hasso-Plattner-Institut fuer Softwaresystemtechnik GmbH. All rights reserved.
#ifndef SRC_LIB_STORAGE_PAGEDINDEX_H_
#define SRC_LIB_STORAGE_PAGEDINDEX_H_

#include <vector>
#include <map>
#include <set>
#include <stdexcept>
#include <algorithm>
#include <iostream>

#include "helper/types.h"

#include "storage/storage_types.h"
#include "storage/AbstractIndex.h"
#include "storage/AbstractTable.h"
#include "storage/DeltaIndex.h"
#include "storage/OrderPreservingDictionary.h"

#include <memory>
#include <boost/dynamic_bitset.hpp>

namespace hyrise {
namespace storage {


class PagedIndex : public AbstractIndex {
private:
  typedef std::vector<boost::dynamic_bitset<>> paged_index_t;
  paged_index_t _index;
  const size_t _pageSize;
  const field_t _column;

public:
  virtual ~PagedIndex() {};

  void shrink() {
    throw std::runtime_error("Shrink not supported for GroupkeyIndex");
  }

  void write_lock() {}
 
  void read_lock() {}

  void unlock() {}

  explicit PagedIndex(const hyrise::storage::c_atable_ptr_t& in, field_t column, size_t pageSize = 4, bool debug = 0): _pageSize(pageSize), _column(column) {
    if (in != nullptr) {

      rebuildIndex(in);

      if (debug)
        debugPrint();
    }
  };

  void rebuildIndex(const hyrise::storage::c_atable_ptr_t& in) {
    if (in != nullptr) {
      size_t num_of_pages = std::ceil(in->size() / (double)_pageSize);

      paged_index_t _newIndex(in->dictionaryAt(_column)->size(), boost::dynamic_bitset<>(num_of_pages, 0));
        
      for (size_t row = 0; row < in->size(); ++row)
        _newIndex[in->getValueId(_column, row).valueId][row/_pageSize] = 1;

      _index = _newIndex;
    }
  }

template<typename T>
  void mergeIndex(size_t newTableSize,
                  const std::set<T>& deltaDict,
                  std::shared_ptr<hyrise::storage::DeltaIndex<T>> deltaIdx,
                  std::shared_ptr<hyrise::storage::OrderPreservingDictionary<T>> resultDict,
                  const std::vector<value_id_t>& mapping) {

    size_t num_of_pages = std::ceil(newTableSize / (double)_pageSize);
    paged_index_t _newIndex(resultDict->size(), boost::dynamic_bitset<>(num_of_pages, 0));
       
    for (size_t i=0; i<mapping.size(); i++) {
      _newIndex[mapping[i]] = _index[i];
      _newIndex[mapping[i]].resize(num_of_pages, 0);
    }

    for (auto d : deltaDict) {
      hyrise::storage::PositionRange positions = deltaIdx->getPositionsForKey(d);
      for (auto j : positions)
        _newIndex[resultDict->getValueIdForValue(d)][std::floor(j / (double) _pageSize)] = 1;
    }

    _index = _newIndex;

    debugPrint();
  }

  /**
   * returns a list of positions where key was found.
   */

template<typename T>
  hyrise::storage::pos_list_t* getPositionsForKey(T key, field_t column, const hyrise::storage::c_atable_ptr_t& table) {
    hyrise::storage::pos_list_t *result = new hyrise::storage::pos_list_t();


    // retrieve valueId for dictionary entry T "key"
    bool value_exists = table->valueExists(column, key);

    if (!value_exists)
      return result;

    ValueId keyValueId;
    keyValueId = table->getValueIdForValue(column, key);

    // scan through pages
    for (size_t i=0; i<_index[keyValueId.valueId].size(); ++i) {
      // skip 0
      if (_index[keyValueId.valueId][i] != 1)
        continue;

      // for every 1, scan through table values, compare them with valueId
      size_t offsetStart = i * _pageSize;
      size_t offsetEnd = std::min<size_t>(offsetStart + _pageSize, table->size());
      std::cout << "okay, index page " << i << " is 1, will scan from " << offsetStart << " to " << offsetEnd << std::endl;
      for (size_t p=offsetStart; p<offsetEnd; ++p) {
        // print valueid here
        std::cout << "valueId at" << p << " " << column << table->getValueId(column, p) <<std::endl;
        std::cout << "value at " << p << " is " << table->getValue<T>(column, p) << std::endl;
        if (value_exists && table->getValueId(column, p) != keyValueId)//(table->getValue<T>(column, p) != key)
          continue;

        std::cout << "--> pushing to result list" << std::endl;
        result->push_back(p);
      }
    }
    return result;
  }

  size_t getPageSize() {
    return _pageSize;
  }

  void debugPrint() {
        std::cout << "page_size: " << _pageSize << std::endl;
        std::cout << "num_of_pages: " << _index.size() << std::endl;
        for (size_t i=0; i<_index.size(); ++i) {
          //std::cout << in->getValueForValueId<T>(column, ValueId(i, 0)) << " - ";
          for (size_t o=0; o<_index[i].size(); ++o) 
            std::cout << _index[i][o];
          std::cout << std::endl;
        }
  }

};

} } // namespace hyrise::storage

#endif  // SRC_LIB_STORAGE_PAGEDINDEX_H_
