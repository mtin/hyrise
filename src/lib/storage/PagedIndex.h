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
#include "storage/OrderIndifferentDictionary.h"

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
  const bool _debug;

public:
  virtual ~PagedIndex() {};

  void shrink() {
    throw std::runtime_error("Shrink not supported for PagedIndex");
  }

  void write_lock() {}
 
  void read_lock() {}

  void unlock() {}

  explicit PagedIndex(const hyrise::storage::c_atable_ptr_t& in, field_t column, size_t pageSize = 512, bool debug = false): 
    _pageSize(pageSize), 
    _column(column),
    _debug(debug) {
    if (in != nullptr) {

      _index = buildIndex(in);

      if (_debug)
        debugPrint();
    }
  };

  /**
   * build index without previous information
   */
  paged_index_t buildIndex(const hyrise::storage::c_atable_ptr_t& in) {
      size_t num_of_pages = std::ceil(in->size() / (double)_pageSize);

      paged_index_t _newIndex(in->dictionaryAt(_column)->size(), boost::dynamic_bitset<>(num_of_pages, 0));
        
      for (size_t row = 0; row < in->size(); ++row)
        _newIndex[in->getValueId(_column, row).valueId][row/_pageSize] = 1;

      return _newIndex;
  }

  /**
   * rebuild index without previous information
   */
  void rebuildIndex(const hyrise::storage::c_atable_ptr_t& in) {
      if (in != nullptr) {
        _index = buildIndex(in);

        if (_debug)
          debugPrint();
      }
  }

  /**
   * use aux. structure from merge process to efficiently rebuild the index
   */ 
template<typename T>
  void mergeIndex(size_t oldTableSize,
                  size_t deltaTableSize,
                  const std::shared_ptr<hyrise::storage::OrderPreservingDictionary<T>>&   resultDict,
                  const std::vector<value_id_t>& mapping,
                  const std::vector<value_id_t>& deltaMapping) {

    size_t num_of_pages = std::ceil((oldTableSize + deltaTableSize) / (double)_pageSize);
    paged_index_t _newIndex(resultDict->size(), boost::dynamic_bitset<>(num_of_pages, 0));
       
    // transfer pages from old index
    for (size_t i=0; i<mapping.size(); i++) {
      _newIndex[mapping[i]] = _index[i];
      _newIndex[mapping[i]].resize(num_of_pages, 0);
    }

    // set page bits for additional entries
    for (size_t i=0; i<deltaMapping.size(); i++) {
      _newIndex[deltaMapping[i]][std::floor((oldTableSize + i) / (double) _pageSize)] = 1;
    } 

    _index = _newIndex;

    if (_debug)
      debugPrintWithValues<T>(resultDict);
  }

  /**
   * returns a list of positions where key was found.
   */
template<typename T>
  hyrise::storage::pos_list_t* getPositionsForKey(T key, field_t column, const hyrise::storage::c_atable_ptr_t& table) {

    // check if given key is valid
    if (!table->valueExists(column, key))
      return new hyrise::storage::pos_list_t();

    // retrieve valueId for dictionary entry T "key"
    ValueId keyValueId = table->getValueIdForValue(column, key);

    return getPositionsForValueId(keyValueId, column, table);
  }

  /**
   * returns a list of positions where keyValueId was found.
   */
  hyrise::storage::pos_list_t* getPositionsForValueId(ValueId keyValueId, field_t column, const hyrise::storage::c_atable_ptr_t& table) {
    hyrise::storage::pos_list_t *result = new hyrise::storage::pos_list_t();

    // scan through pages
    for (size_t i=0; i<_index[keyValueId.valueId].size(); ++i) {
      // skip 0
      if (_index[keyValueId.valueId][i] != 1)
        continue;

      // for every 1, calc range of rows in page
      size_t offsetStart = i * _pageSize;
      size_t offsetEnd = std::min<size_t>(offsetStart + _pageSize, table->size());
      
      // scan through table value-ids, compare them with keyValueId
      for (size_t p=offsetStart; p<offsetEnd; ++p) {
        if (table->getValueId(column, p) != keyValueId)//(table->getValue<T>(column, p) != key)
          continue;

        result->push_back(p);
      }
    }
    return result;
  }

  /**
   * returns the page size
   */
  size_t getPageSize() {
    return _pageSize;
  }

  /**
   * returns the column the index was created on
   */
  field_t getColumn() {
    return _column;
  }

  /**
   * prints the index
   */
  void debugPrint() {
        std::cout << "page_size: " << _pageSize << std::endl;
        std::cout << "num_of_pages: " << ((_index.size() > 0) ? _index[0].size() : 0) << std::endl;
        for (size_t i=0; i<_index.size(); ++i) {
          for (size_t o=0; o<_index[i].size(); ++o) 
            std::cout << _index[i][o];
          std::cout << std::endl;
        }
  }
  /**
   * prints the index plus resolves value-ids
   */
template <typename T>
  void debugPrintWithValues(std::shared_ptr<hyrise::storage::OrderPreservingDictionary<T>> resultDict) {
        std::cout << "page_size: " << _pageSize << std::endl;
        std::cout << "num_of_pages: " << ((_index.size() > 0) ? _index[0].size() : 0) << std::endl;
        for (size_t i=0; i<_index.size(); ++i) {
          for (size_t o=0; o<_index[i].size(); ++o) 
            std::cout << _index[i][o];
          std::cout << " (" << resultDict->getValueForValueId(i) << ")" << std::endl;
        }
  }

};

} } // namespace hyrise::storage

#endif  // SRC_LIB_STORAGE_PAGEDINDEX_H_
