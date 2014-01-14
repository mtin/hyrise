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

class PagedIndex : public hyrise::storage::AbstractIndex {
private:
  typedef std::map<ValueId, boost::dynamic_bitset<>> paged_index_t;
  paged_index_t _index;
  size_t _pageSize;

public:
  virtual ~PagedIndex() {};

  void shrink() {
      std::cout << "SHRINK NOT IMPLEMENTED YEY: " <<std::endl;
//for (auto & e : _index)
      //e.second.shrink_to_fit();
  }

  explicit PagedIndex(const hyrise::storage::c_atable_ptr_t& in, field_t column, size_t pageSize = 4, bool debug = 0): _pageSize(pageSize) {
    if (in != nullptr) {
      size_t num_of_pages = std::ceil(in->size() / (double)_pageSize);
      if (debug) {
        std::cout << "table_size: " << in->size() << std::endl;
        std::cout << "page_size: " << _pageSize << std::endl;
        std::cout << "num_of_pages: " << num_of_pages << std::endl;
      }
      
        
      for (size_t row = 0; row < in->size(); ++row) {
        ValueId valueId = in->getValueId(column, row);

        typename paged_index_t::iterator find = _index.find(valueId);
        if (find == _index.end()) {
          boost::dynamic_bitset<> b(num_of_pages, 0); // create bitset with 1 bit per page
          b[row/_pageSize] = 1; // set bit to 1 for page where we found the current entry
          _index[valueId] = b; // add bitset to index
        } else {
          find->second[row/_pageSize] = 1;
        }
      }

      if (debug) {
        for (const auto & e : _index) {
          //in->getValueForValueId(column, e.first);
          std::cout << e.first << " - ";
          for (size_t i=0; i<e.second.size(); ++i) 
            std::cout << e.second[i];
          std::cout << std::endl;
        }
      }
    }
  };

  /**
   * returns a list of positions where key was found.
   */
template<typename T>
  boost::dynamic_bitset<> getPagesForKey(T key) {
    typename paged_index_t::iterator it = _index.find(key);
    if (it != _index.end()) {
      return it->second;
    } else {
      boost::dynamic_bitset<> empty;
      return empty;
    }
  };

template<typename T>
  hyrise::storage::pos_list_t* getPositionsForKey(T key, field_t column, const hyrise::storage::c_atable_ptr_t& table) {
    hyrise::storage::pos_list_t *result = new hyrise::storage::pos_list_t();


    // retrieve valueId for dictionary entry T "key"
    bool value_exists = table->valueExists(column, key);

    if (!value_exists)
      return result;

    ValueId keyValueId;
    keyValueId = table->getValueIdForValue(column, key);

    // look for key in index
    typename paged_index_t::iterator it = _index.find(keyValueId);
    if (it == _index.end())
      return result;

    // scan through pages
    for (size_t i=0; i<it->second.size(); ++i) {
      // skip 0
      if (it->second[i] != 1)
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
#endif  // SRC_LIB_STORAGE_PAGEDINDEX_H_
