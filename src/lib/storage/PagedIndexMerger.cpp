// Copyright (c) 2012 Hasso-Plattner-Institut fuer Softwaresystemtechnik GmbH. All rights reserved.
#include "PagedIndexMerger.h"

#include <memory>
#include <set>

#include "OrderPreservingDictionary.h"
#include "OrderIndifferentDictionary.h"
#include "meta_storage.h"
#include "RawTable.h"
#include "SimpleStore.h"

#include "io/StorageManager.h" //MF
#include "storage/PagedIndex.h" //MF
#include "storage/DeltaIndex.h" //MF


// find the index merge below at line 104++


namespace hyrise { namespace storage {

/**
 * This class implements the part for merging the dictionaries between
 * the uncompressed delta and the compressed order preserving
 * dictionary. This is done using the following simple
 * algorithm. First a new std::set is created to store all distinct
 * values in a sorted order. Now a new dictionary is created based on
 * this set and a mapping table is build that maps the old values from
 * the old dictionary to the new dictionary.
 */
struct IndexMergeDictFunctorPaged {

  struct result {
    std::shared_ptr<std::vector<value_id_t>> mapping;
    std::shared_ptr<AbstractDictionary> dict;
  };

  // Result type value definition
  typedef result value_type;

  c_atable_ptr_t _main;
  c_atable_ptr_t _delta;
  field_t _column;

  void prepare(c_atable_ptr_t m, c_atable_ptr_t d, field_t c) {
    _main = m;
    _delta = d;
    _column = c;
  }
  
  template<typename R>
  result operator()() {
    auto dict = std::dynamic_pointer_cast<OrderPreservingDictionary<R>>(_main->dictionaryAt(_column));
    std::set<R> data;

    // Build unified dictionary
    size_t deltaSize = _delta->size();
    for(size_t i=0; i < deltaSize; ++i) {
      data.insert(_delta->getValue<R>(_column, i));
    }

    size_t dictSize = dict->size();
    for(size_t i=0; i < dictSize; ++i)
      data.insert(dict->getValueForValueId(i));

    // Build mapping table for old dictionary
    auto start = data.cbegin();
    auto end = data.cend();
    size_t mapped = 0;
    
    std::shared_ptr<std::vector<value_id_t>> mapping = std::make_shared<std::vector<value_id_t>>();

    for(size_t i=0; i < dictSize; ++i) {
      auto val = dict->getValueForValueId(i);

      // Skip until we are equal
      while(start != end && *start != val) {
        ++mapped; ++start;
      }

      if (start != end)
        ++start;

      mapping->push_back(mapped++);
    }

    auto resultDict = std::make_shared<OrderPreservingDictionary<R>>(data.size());
    for(auto e : data)
      resultDict->addValue(e);

    result r = {std::move(mapping), std::move(resultDict)};
    return r;
  }

};

/**
 * This class performs the mapping of old uncompressed delta values to
 * new valueIds in the compressed data store.
 */
struct MapValueForValueIdPaged {
  typedef void value_type;

  atable_ptr_t _main;
  std::shared_ptr<AbstractDictionary> _dict;
  c_atable_ptr_t _delta;
  field_t _col;
  field_t _dstCol;

  //MF
  std::shared_ptr<hyrise::storage::PagedIndex> _index;
  std::shared_ptr<std::vector<value_id_t>> _mapping;

  void prepare(atable_ptr_t m, field_t dst, std::shared_ptr<AbstractDictionary> d, 
               size_t col, c_atable_ptr_t de) {
    _main = m;
    _dstCol = dst;
    _dict = d;
    _delta = de;
    _col = col;
  }

  //MF
  void prepareIndexMerge(const std::shared_ptr<PagedIndex>& index,
                         const std::shared_ptr<std::vector<value_id_t>>& mapping) {
    _index = index;
    _mapping = mapping;
  }

  template<typename R>
  value_type operator() () {

    auto resultDict = std::dynamic_pointer_cast<OrderPreservingDictionary<R>>(_dict);
    size_t tabSize = _main->size();
    size_t start = _main->size() - _delta->size();
    std::vector<value_id_t> deltaMapping; // MF
    for(size_t row = start; row < tabSize; ++row) { 
      deltaMapping.push_back(resultDict->getValueIdForValue(_delta->getValue<R>(_col, row-start))); // MF
      _main->setValueId(_dstCol, row, ValueId{deltaMapping.back(), 0});
    }

    // MF check if this is the column our index is working on (otherwise index is nullptr)
    if (!_index) {
      return;
    }

    // MF merge index
    _index->mergeIndex(_main->size() - _delta->size(), 
                       _delta->size(), 
                       resultDict, 
                       *_mapping, 
                       deltaMapping);

    _index = nullptr;
  }
};


PagedIndexMerger::PagedIndexMerger(std::string index_name):
  _index_name(index_name) {} //MF

void PagedIndexMerger::mergeValues(const std::vector<c_atable_ptr_t > &input_tables,
                              atable_ptr_t merged_table,
                              const column_mapping_t &column_mapping,
                              const uint64_t newSize,
                              bool useValid,
                              const std::vector<bool>& valid) {

  if (useValid)
    throw std::runtime_error("PagedIndexMerger does not support valid vectors");

  if(input_tables.size() != 2) throw std::runtime_error("PagedIndexMerger does not support more than two tables");
  auto delta = input_tables[1];
  auto main = input_tables[0];

  // Prepare type handling
  IndexMergeDictFunctorPaged fun;
  type_switch<hyrise_basic_types> ts;

  std::vector<IndexMergeDictFunctorPaged::result> mergedDictionaries(column_mapping.size());

  // Extract unique values for delta
  for(const auto& kv : column_mapping) {
    const auto& col = kv.first;
    const auto& dst = kv.second;
    fun.prepare(main, delta, col);
    auto result = ts(main->typeOfColumn(col), fun);
    merged_table->setDictionaryAt(result.dict, dst);
    mergedDictionaries[col] = result;
  }

  // Update the values of the new Table
  merged_table->resize(newSize);
  size_t tabSize = main->size();
  for(size_t row=0; row < tabSize; ++row) {
    for( const auto& kv : column_mapping) {
      const auto& col = kv.first;
      const auto& dst = kv.second;
      merged_table->setValueId(dst, row, ValueId{(*mergedDictionaries[col].mapping)[main->getValueId(col, row).valueId], 0});
    }
  }
  
  //MF get main index
  hyrise::io::StorageManager *sm = hyrise::io::StorageManager::getInstance();
  auto idx = sm->getInvertedIndex(_index_name);
  auto pagedIdx = std::dynamic_pointer_cast<hyrise::storage::PagedIndex>(idx);

  // Map the values for the values in the uncompressed delta
  MapValueForValueIdPaged map;
  for( const auto& kv : column_mapping) {
    const auto& col = kv.first;
    const auto& dst = kv.second;
    map.prepare(merged_table, dst, mergedDictionaries[col].dict, col, delta);
    // MF check if this is the column of our index
    if (pagedIdx->getColumn() == dst) {
      map.prepareIndexMerge(pagedIdx, mergedDictionaries[col].mapping);
    }
    ts(merged_table->typeOfColumn(dst), map);
  }
}

}}

