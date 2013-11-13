// Copyright (c) 2012 Hasso-Plattner-Institut fuer Softwaresystemtechnik GmbH. All rights reserved.
#include "storage/PointerCalculator.h"

#include <iostream>
#include <string>
#include <unordered_set>

#include "helper/checked_cast.h"

#include "storage/PrettyPrinter.h"
#include "storage/Store.h"
#include "storage/TableRangeView.h"

using namespace hyrise::storage;

template <typename T>
T* copy_vec(T* orig) {
  if (orig == nullptr) return nullptr;
  return new T(begin(*orig), end(*orig));
}

PointerCalculator::PointerCalculator(hyrise::storage::c_atable_ptr_t t, pos_list_t *pos, field_list_t *f) : table(t), pos_list(pos), fields(f) {
  // prevent nested pos_list/fields: if the input table is a
  // PointerCalculator instance, combine the old and new
  // pos_list/fields lists
  if (auto p = std::dynamic_pointer_cast<const PointerCalculator>(t)) {
    if (pos_list != nullptr && p->pos_list != nullptr) {
      pos_list = new pos_list_t(pos->size());
      for (size_t i = 0; i < pos->size(); i++) {
        (*pos_list)[i] = p->pos_list->at(pos->at(i));
      }
      table = p->table;
      delete pos;
    }
    if (fields != nullptr && p->fields != nullptr) {
      fields = new field_list_t(f->size());
      for (size_t i = 0; i < f->size(); i++) {
        (*fields)[i] = p->fields->at(f->at(i));
      }
      table = p->table;
    }
  }
  if (auto trv = std::dynamic_pointer_cast<const TableRangeView>(t)){
    const auto start =  trv->getStart();
    if (pos_list != nullptr && start != 0) {
      for (size_t i = 0; i < pos->size(); i++) {
        (*pos_list)[i] += start;
      }
    }
    table = trv->getTable();
  }
  updateFieldMapping();
}

PointerCalculator::PointerCalculator(const PointerCalculator& other) : table(other.table), pos_list(copy_vec(other.pos_list)), fields(copy_vec(other.fields)) {
  updateFieldMapping();
}

hyrise::storage::atable_ptr_t PointerCalculator::copy() const {
  return create(table, fields, pos_list);
}

PointerCalculator::~PointerCalculator() {
  // delete fields;
  // delete pos_list;
}

void PointerCalculator::updateFieldMapping() {
  slice_for_slice.clear();
  offset_in_slice.clear();
  width_for_slice.clear();
  slice_count = 0;

  size_t field = 0, dst_field = 0;
  size_t last_field = 0;
  bool last_field_set = false;

  for (size_t src_slice = 0; src_slice < table->partitionCount(); src_slice++) {
    last_field_set = false;
    for (size_t src_field = 0; src_field < table->partitionWidth(src_slice); src_field++) {
      // Check if we have to increase the fields until we reach
      // a projected attribute
      if (fields && (fields->size() <= dst_field || fields->at(dst_field) != field)) {
        field++;
        continue;
      }

      if (!last_field_set || field > last_field + 1) { // new slice
        slice_count++;
        slice_for_slice.push_back(src_slice);
        offset_in_slice.push_back(src_field);
        width_for_slice.push_back(1);
      } else {
        width_for_slice[slice_count - 1] += 1;
      }

      dst_field++;
      last_field = field;

      if (!last_field_set) {
        last_field_set = true;
      }

      field++;
    }
  }
}

void PointerCalculator::setPositions(const pos_list_t pos) {
  if (pos_list != nullptr)
    delete pos_list;
  pos_list = new std::vector<pos_t>(pos);
}

void PointerCalculator::setFields(const field_list_t f) {
  fields = new std::vector<field_t>(f);
  updateFieldMapping();
}

const ColumnMetadata *PointerCalculator::metadataAt(const size_t column_index, const size_t row_index, const table_id_t table_id) const {
  size_t actual_column;

  if (fields) {
    actual_column = fields->at(column_index);
  } else {
    actual_column = column_index;
  }

  return table->metadataAt(actual_column);
}

void PointerCalculator::setDictionaryAt(AbstractTable::SharedDictionaryPtr dict, const size_t column, const size_t row, const table_id_t table_id) {
  throw std::runtime_error("Can't set PointerCalculator dictionary");
}

const AbstractTable::SharedDictionaryPtr& PointerCalculator::dictionaryAt(const size_t column, const size_t row, const table_id_t table_id) const {
  size_t actual_column, actual_row;

  if (fields) {
    actual_column = fields->at(column);
  } else {
    actual_column = column;
  }

  if (pos_list && pos_list->size() > 0) {
    actual_row = pos_list->at(row);
  } else {
    actual_row = row;
  }

  return table->dictionaryAt(actual_column, actual_row, table_id);
}

const AbstractTable::SharedDictionaryPtr& PointerCalculator::dictionaryByTableId(const size_t column, const table_id_t table_id) const {
  size_t actual_column;

  if (fields) {
    actual_column = fields->at(column);
  } else {
    actual_column = column;
  }

  return table->dictionaryByTableId(actual_column, table_id);
}

size_t PointerCalculator::size() const {
  if (pos_list) {
    return pos_list->size();
  }

  return table->size();
}

size_t PointerCalculator::columnCount() const {
  if (fields) {
    return fields->size();
  }

  return table->columnCount();
}

ValueId PointerCalculator::getValueId(const size_t column, const size_t row) const {
  size_t actual_column, actual_row;

  if (pos_list) {
    actual_row = pos_list->at(row);
  } else {
    actual_row = row;
  }

  if (fields) {
    actual_column = fields->at(column);
  } else {
    actual_column = column;
  }

  return table->getValueId(actual_column, actual_row);
}

unsigned PointerCalculator::partitionCount() const {
  return slice_count;
}

size_t PointerCalculator::partitionWidth(const size_t slice) const {
  // FIXME this should return the width in bytes for the column
  if (fields) {
    return width_for_slice[slice];
  }

  return table->partitionWidth(slice);
}


void PointerCalculator::print(const size_t limit) const {
  PrettyPrinter::print(this, std::cout, "unnamed pointer calculator", limit);
}

size_t PointerCalculator::getTableRowForRow(const size_t row) const
{
  size_t actual_row;
  // resolve mapping of THIS pointer calculator
  if (pos_list) {
    actual_row = pos_list->at(row);
  } else {
    actual_row = row;
  }
  // if underlying table is PointerCalculator, resolve recursively
  auto p = std::dynamic_pointer_cast<const PointerCalculator>(table);
  if (p)
    actual_row = p->getTableRowForRow(actual_row);

  return actual_row;
}

size_t PointerCalculator::getTableColumnForColumn(const size_t column) const
{
  size_t actual_column;
  // resolve field mapping of THIS pointer calculator
  if (fields) {
    actual_column = fields->at(column);
  } else {
    actual_column = column;
  }
  // if underlying table is PointerCalculator, resolve recursively
  auto p = std::dynamic_pointer_cast<const PointerCalculator>(table);
  if (p)
    actual_column = p->getTableColumnForColumn(actual_column);
  return actual_column;
}

hyrise::storage::c_atable_ptr_t PointerCalculator::getTable() const {
  return table;
}

hyrise::storage::c_atable_ptr_t PointerCalculator::getActualTable() const {
  auto p = std::dynamic_pointer_cast<const PointerCalculator>(table);

  if (!p) {
    return table;
  } else {
    return p->getActualTable();
  }
}

const pos_list_t *PointerCalculator::getPositions() const {
  return pos_list;
}

pos_list_t PointerCalculator::getActualTablePositions() const {
  auto p = std::dynamic_pointer_cast<const PointerCalculator>(table);

  if (!p) {
    return *pos_list;
  }

  pos_list_t result(pos_list->size());
  pos_list_t positions = p->getActualTablePositions();

  for (pos_list_t::const_iterator it = pos_list->begin(); it != pos_list->end(); ++it) {
    result.push_back(positions[*it]);
  }

  return result;
}


//FIXME: Template this method
hyrise::storage::atable_ptr_t PointerCalculator::copy_structure(const field_list_t *fields, const bool reuse_dict, const size_t initial_size, const bool with_containers, const bool compressed) const {
  std::vector<const ColumnMetadata *> metadata;
  std::vector<AbstractTable::SharedDictionaryPtr> *dictionaries = nullptr;

  if (reuse_dict) {
    dictionaries = new std::vector<AbstractTable::SharedDictionaryPtr>();
  }

  if (fields != nullptr) {
    for (const field_t & field: *fields) {
      metadata.push_back(metadataAt(field));

      if (dictionaries != nullptr) {
        dictionaries->push_back(dictionaryAt(field, 0, 0));
      }
    }
  } else {
    for (size_t i = 0; i < columnCount(); ++i) {
      metadata.push_back(metadataAt(i));

      if (dictionaries != nullptr) {
        dictionaries->push_back(dictionaryAt(i, 0, 0));
      }
    }
  }

  auto result = std::make_shared<Table>(&metadata, dictionaries, initial_size, true, compressed);
  delete dictionaries;
  return result;
}

void PointerCalculator::intersect_pos_list(
    pos_list_t::iterator beg1, pos_list_t::iterator end1,
    pos_list_t::iterator beg2, pos_list_t::iterator end2,
    pos_list_t *result, bool first_sorted, bool second_sorted) {

  // produces sorted intersection of two pos lists. based on
  // baeza-yates algorithm with average complexity nicely
  // adapting to the smaller list size. whereas std::set_intersection
  // iterates over both lists with linear complexity.

  if (!first_sorted) {
    // copy input 1 and sort it
    pos_list_t input1_sorted; input1_sorted.reserve(end1-beg1);
    std::copy ( beg1, end1, std::back_inserter(input1_sorted) );
    std::sort ( input1_sorted.begin(), input1_sorted.end() );
    beg1 = input1_sorted.begin();
    end1 = input1_sorted.end();
  }
  if (!second_sorted) {
    // copy input 2 and sort it
    pos_list_t input2_sorted; input2_sorted.reserve(end2-beg2);
    std::copy ( beg2, end2, std::back_inserter(input2_sorted) );
    std::sort ( input2_sorted.begin(), input2_sorted.end() );
    beg2 = input2_sorted.begin();
    end2 = input2_sorted.end();
  }

  int size_1 = end1-beg1;
  int size_2 = end2-beg2;

  // if one of the inputs is empty,
  // return as intersect is empty
  if (size_1<=0 or size_2<=0) return;

  // if input 1 and input 2 do not overlap at all,
  // return as intersect is empty
  if (*(end1-1) < *beg2 or *(end2-1) < *beg1) return;

  // if both lists are very small,
  // use std intersaction as iterating is faster than binary search
  if (size_1+size_2 < 20) {
    std::set_intersection(beg1, end1, beg2, end2, std::back_inserter(*result));
    return;
  }

  // make sure input 1 is larger than input 2
  if (size_1<size_2) {
    std::swap<pos_list_t::iterator>(end1, end2);
    std::swap<pos_list_t::iterator>(beg1, beg2);
    std::swap<int>(size_1, size_2);
  }

  // find overlap by searching in smaller input (input 2)
  beg2 = std::lower_bound(beg2, end2, *beg1);
  end2 = std::upper_bound(beg2, end2, *(end1-1));
  size_2 = end2-beg2;
  
  // search median of input 2 in input 1 
  // effectively dividing larger input in two
  auto m = beg2 + (size_2/2);
  auto m_in_1 = std::lower_bound(beg1, end1, *m);

  // and recursively do the rest
  if (*m_in_1==*m) {
    PointerCalculator::intersect_pos_list(beg1, m_in_1, beg2, m, result);
    result->push_back(*m);
    PointerCalculator::intersect_pos_list(m_in_1+1, end1, m+1, end2, result);
  } else {
    PointerCalculator::intersect_pos_list(beg1, m_in_1, beg2, m, result);
    PointerCalculator::intersect_pos_list(m_in_1, end1, m+1, end2, result);
  }
}


std::shared_ptr<PointerCalculator> PointerCalculator::intersect(const std::shared_ptr<const PointerCalculator>& other) const {
  pos_list_t *result = new pos_list_t();
  result->reserve(std::max(pos_list->size(), other->pos_list->size()));
  assert(std::is_sorted(begin(*pos_list), end(*pos_list)) && std::is_sorted(begin(*other->pos_list), end(*other->pos_list)) && "Both lists have to be sorted");
  
  PointerCalculator::intersect_pos_list(
    pos_list->begin(), pos_list->end(),
    other->pos_list->begin(), other->pos_list->end(),
    result);

  assert((other->table == this->table) && "Should point to same table");
  return create(table, result, fields);
}


bool PointerCalculator::isSmaller( std::shared_ptr<const PointerCalculator> lx, std::shared_ptr<const PointerCalculator> rx ) {
  return lx->size() < rx->size() ;
}

std::shared_ptr<const PointerCalculator> PointerCalculator::intersect_many(pc_vector::iterator it, pc_vector::iterator it_end) {
  // pc_vector pcs;
  // pcs.resize(it_end-it);
  // std::copy ( it, it_end, pcs.begin() );
  std::sort(it, it_end, PointerCalculator::isSmaller);

  // pc_vector::const_iterator pcs_it = pcs.begin();
  // pc_vector::const_iterator pcs_end = pcs.end();

  std::shared_ptr<const PointerCalculator> base = *(it++);
  for (;it != it_end; ++it) {
    base = base->intersect(*it);
  }
  return base;
}

std::shared_ptr<PointerCalculator> PointerCalculator::unite(const std::shared_ptr<const PointerCalculator>& other) const {
  assert((other->table == this->table) && "Should point to same table");
  if (pos_list && other->pos_list) {
    auto result = new pos_list_t();
    result->reserve(pos_list->size() + other->pos_list->size());
    assert(std::is_sorted(begin(*pos_list), end(*pos_list)) && std::is_sorted(begin(*other->pos_list), end(*other->pos_list)) && "Both lists have to be sorted");
    std::set_union(pos_list->begin(), pos_list->end(),
                   other->pos_list->begin(), other->pos_list->end(),
                   std::back_inserter(*result));
    return create(table, result, copy_vec(fields));
  } else {
    pos_list_t* positions = nullptr;
    if (pos_list == nullptr) { positions = other->pos_list; }
    if (other->pos_list == nullptr) { positions = pos_list; }
    return create(table, copy_vec(positions), copy_vec(fields));
  }
}

std::shared_ptr<PointerCalculator> PointerCalculator::concatenate(const std::shared_ptr<const PointerCalculator>& other) const {
  assert((other->table == this->table) && "Should point to same table");
  std::vector<std::shared_ptr<const PointerCalculator>> v {std::static_pointer_cast<const PointerCalculator>(shared_from_this()), other};
  return PointerCalculator::concatenate_many(begin(v), end(v));
}

std::shared_ptr<const PointerCalculator> PointerCalculator::unite_many(pc_vector::const_iterator it, pc_vector::const_iterator it_end){
  std::shared_ptr<const PointerCalculator> base = *(it++);
  for (;it != it_end; ++it) {
    base = base->unite(*it);
  }
  return base;
}

std::shared_ptr<PointerCalculator> PointerCalculator::concatenate_many(pc_vector::const_iterator it, pc_vector::const_iterator it_end) {
  auto sz = std::accumulate(it, it_end, 0, [] (size_t acc, const std::shared_ptr<const PointerCalculator>& pc) { return acc + pc->size(); });
  auto result = new pos_list_t;
  result->reserve(sz);

  hyrise::storage::c_atable_ptr_t table = nullptr;
  for (;it != it_end; ++it) {
    const auto& pl = (*it)->pos_list;
    if (table == nullptr) {
      table = (*it)->table;
    }

    if (pl == nullptr) {
      auto sz = (*it)->size();
      result->resize(result->size() + sz);
      std::iota(end(*result)-sz, end(*result), 0);
    } else {
      result->insert(end(*result), begin(*pl), end(*pl));
    }
  }

  return create(table, result, nullptr);
}

void PointerCalculator::debugStructure(size_t level) const {
  std::cout << std::string(level, '\t') << "PointerCalculator " << this << std::endl;
  table->debugStructure(level+1);
}


void PointerCalculator::validate(hyrise::tx::transaction_id_t tid, hyrise::tx::transaction_id_t cid) {
  const auto& store = checked_pointer_cast<const hyrise::storage::Store>(table);
  if (pos_list == nullptr) {
    pos_list = new pos_list_t(store->buildValidPositions(cid, tid));
  } else {
    store->validatePositions(*pos_list, cid, tid);
  }
}

void PointerCalculator::remove(const pos_list_t& pl) {
  std::unordered_set<pos_t> tmp(pl.begin(), pl.end());
  const auto& end = tmp.cend();
  auto res = std::remove_if(std::begin(*pos_list), std::end(*pos_list),[&tmp, &end](const pos_t& p){
    return tmp.count(p) != 0u;
  });
  (*pos_list).erase(res, pos_list->end());
}
