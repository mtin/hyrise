#ifdef PERSISTENCY_NVRAM

// Copyright (c) 2012 Hasso-Plattner-Institut fuer Softwaresystemtechnik GmbH. All rights reserved.
#ifndef SRC_LIB_STORAGE_NVATTRIBUTEVECTOR_H_
#define SRC_LIB_STORAGE_NVATTRIBUTEVECTOR_H_

#include "storage/FixedLengthVector.h"
#include "storage/NVVector.h"

template <typename T>
class NVAttributeVector : public AbstractFixedLengthVector<T> {
 private:
  hyrise::storage::NVVector<T> _vector;
  size_t _columns;

 public:
  typedef T value_type;

  NVAttributeVector(size_t columns, size_t rows) :
      _vector(columns * rows), _columns(columns) {
    _vector.clear(); // we don't want our vector to be initialized with values
  }

  void *data() {
    return &_vector.front();
  }

  void setNumRows(size_t s) {
    _vector.reserve(s * _columns);
  }

  inline T get(size_t column, size_t row) const {
    checkAccess(column, row);
    return _vector[row * _columns + column];
  }

  const T& getRef(size_t column, size_t row) const {
    checkAccess(column, row);
    return _vector[row * _columns + column];
  }

  inline void set(size_t column, size_t row, T value) {
    checkAccess(column, row);
    _vector[row * _columns + column] = value;
  }

  void reserve(size_t rows) {
    _vector.reserve(_columns * rows);
  }

  void clear() {
    _vector.clear();
  }

  size_t size() {
    return _vector.size() / _columns;
  }

  void resize(size_t rows) {
    _vector.resize(rows * _columns);
  }

  void rewriteColumn(const size_t column, const size_t bits) {}

  // returns the capacity of the container
  inline uint64_t capacity() {
    return _vector.capacity() / _columns;
  }

  std::shared_ptr<BaseAttributeVector<T>> copy() {
    throw std::runtime_error("NVAttributeVector::copy() is not implemented yet as its behavior is unclear");
  }

  // Increment the value by 1
  T inc(size_t column, size_t row) {
    checkAccess(column, row);
    return _vector[row * _columns + column]++;
  }

  // Atomic Increment the value by one
  T atomic_inc(size_t column, size_t row) {
    checkAccess(column, row);
    return __sync_fetch_and_add(&_vector[row * _columns + column], 1);
  }


  const std::string print() {
    std::stringstream buf;
    buf << "Table: " << this << " --- " << std::endl;
    for(size_t i=0; i < size(); ++i) {
      buf << "| ";
      for(size_t j=0; j < _columns; ++j)
        buf << get(j, i) << " |";
      buf << std::endl;
    }
    buf << this << " ---" << std::endl;;
    return buf.str();
  }

  void persist_scattered(const pos_list_t& elements, bool new_elements = true) {
    _vector.persist_scattered(elements, new_elements);
  }

private:
  void checkAccess(const size_t& column, const size_t& rows) const {
#ifdef EXPENSIVE_ASSERTIONS
    if (column >= _columns) {
      throw std::out_of_range("NV: Trying to access column '"
                              + std::to_string(column) + "' where only '"
                              + std::to_string(_columns) + "' available");
    }
    if (rows >= _vector.size() / _columns) {
      throw std::out_of_range("NV: Trying to access row '"
                              + std::to_string(rows) + "' where only '"
                              + std::to_string(_vector.size() / _columns) + "' available");
    }
#endif
  }
};

#endif  // SRC_LIB_STORAGE_NVATTRIBUTEVECTOR_H_
#endif

