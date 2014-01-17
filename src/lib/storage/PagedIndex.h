// Copyright (c) 2012 Hasso-Plattner-Institut fuer Softwaresystemtechnik GmbH. All rights reserved.
#ifndef SRC_LIB_STORAGE_PAGEDINDEX_H_
#define SRC_LIB_STORAGE_PAGEDINDEX_H_

#include <vector>
#include <map>
#include <stdexcept>
#include <algorithm>
#include <iostream>

#include "helper/types.h"

#include "storage/storage_types.h"
#include "storage/AbstractIndex.h"
#include "storage/AbstractTable.h"

#include <memory>
#include <boost/dynamic_bitset.hpp>

namespace hyrise {
namespace storage {


class PagedIndex : public hyrise::storage::AbstractIndex {
private:
  typedef std::vector<boost::dynamic_bitset<>> paged_index_t;
  paged_index_t _index;
  const size_t _pageSize;
  const field_t _column;

public:
  virtual ~PagedIndex() {};

  void shrink() {
      std::cout << "SHRINK NOT IMPLEMENTED YEY: " <<std::endl;
//for (auto & e : _index)
      //e.second.shrink_to_fit();
  }

  explicit PagedIndex(const hyrise::storage::c_atable_ptr_t& in, field_t column, size_t pageSize = 4, bool debug = 0): _pageSize(pageSize), _column(column) {
    if (in != nullptr) {
      if (debug) {
        size_t num_of_pages = std::ceil(in->size() / (double)_pageSize);
        std::cout << "table_size: " << in->size() << std::endl;
        std::cout << "page_size: " << _pageSize << std::endl;
        std::cout << "num_of_pages: " << num_of_pages << std::endl;
      }

      rebuildIndex(in);

      if (debug) {
        for (size_t i=0; i<_index.size(); ++i) {
          //std::cout << in->getValueForValueId<T>(column, ValueId(i, 0)) << " - ";
          for (size_t o=0; o<_index[i].size(); ++o) 
            std::cout << _index[i][o];
          std::cout << std::endl;
        }
      }
    }
  };

  void rebuildIndex(const hyrise::storage::c_atable_ptr_t& in) {
    if (in != nullptr) {
      size_t num_of_pages = std::ceil(in->size() / (double)_pageSize);

      _index.resize(in->dictionaryAt(_column)->size(), boost::dynamic_bitset<>(num_of_pages, 0));
        
      for (size_t row = 0; row < in->size(); ++row) {
        ValueId valueId = in->getValueId(_column, row);

        _index[valueId.valueId][row/_pageSize] = 1;
      }
    }
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
      for (size_t p=offsetStart; p<offsetEnd; ++p) {
        if (value_exists && table->getValueId(column, p) != keyValueId)//(table->getValue<T>(column, p) != key)
          continue;

        result->push_back(p);
      }
    }
    return result;
  }

  size_t getPageSize() {
    return _pageSize;
  }

};

} } // namespace hyrise::storage

#endif  // SRC_LIB_STORAGE_PAGEDINDEX_H_
