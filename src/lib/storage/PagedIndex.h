// Copyright (c) 2012 Hasso-Plattner-Institut fuer Softwaresystemtechnik GmbH. All rights reserved.
#ifndef SRC_LIB_STORAGE_PAGEDINDEX_H_
#define SRC_LIB_STORAGE_PAGEDINDEX_H_

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
class PagedIndex : public AbstractIndex {
private:
  typedef std::vector<bool> bitvector;
  typedef std::map<T, bitvector> paged_index_t;
  paged_index_t _index;
  size_t _pageSize;

public:
  virtual ~PagedIndex() {};

  void shrink() {
      std::cout << "SHRINK NOT IMPLEMENTED YEY: " <<std::endl;
//for (auto & e : _index)
      //e.second.shrink_to_fit();
  }

  explicit PagedIndex(const hyrise::storage::c_atable_ptr_t& in, field_t column, size_t pageSize = 4): _pageSize(pageSize) {
    if (in != nullptr) {
      size_t num_of_pages = std::ceil(in->size() / (double)_pageSize);
      // std::cout << "table_size: " << in->size() << std::endl;
      // std::cout << "page_size: " << _pageSize << std::endl;
      // std::cout << "num_of_pages: " << num_of_pages << std::endl;
      
        
      for (size_t row = 0; row < in->size(); ++row) {
        T tmp = in->getValue<T>(column, row);
        // if (b[row/_pageSize] > 0) // optimization?
        //   continue;
        typename paged_index_t::iterator find = _index.find(tmp);
        if (find == _index.end()) {
          bitvector b(num_of_pages, 0); // create bitvector with 1 bit per page
          b[row/_pageSize] = 1; // set bit to 1 for page where we found the current entry
          _index[tmp] = b;
        } else {
          find->second[row/_pageSize] = 1;
        }
      }

      // for (const auto & e : _index) {
      //   std::cout << e.first << " - ";
      //   for (const auto & t : e.second)
      //     std::cout << t;
      //   std::cout << std::endl;
      // }
    }
  };

  /**
   * returns a list of positions where key was found.
   */
  std::vector<bool> getPagesForKey(T key) {
    typename paged_index_t::iterator it = _index.find(key);
    if (it != _index.end()) {
      return it->second;
    } else {
      std::vector<bool> empty;
      return empty;
    }
  };

  size_t getPageSize() {
    return _pageSize;
  }

};
#endif  // SRC_LIB_STORAGE_PAGEDINDEX_H_
