#ifdef PERSISTENCY_NVRAM

#ifndef SRC_LIB_STORAGE_NVVECTOR_H
#define SRC_LIB_STORAGE_NVVECTOR_H

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <stdexcept>

#include "io/NVManager.h"
#include "io/NVSimulator.h"

namespace hyrise {
namespace storage {

template <typename T>
class NVVector {
public:
  // member types
  typedef T                 value_type;
  typedef size_t            size_type;
  typedef ptrdiff_t         difference_type;
  typedef value_type&       reference;
  typedef const value_type& const_reference;
  typedef value_type*       iterator;
  typedef const value_type* const_iterator;

  // constructors and assignment
  NVVector(size_type count = 0, const_reference value = value_type()) {
    _info = io::NVManager::getInstance().getOrCreateVectorSpace(0, (count > 100000 ? count : 100000) * sizeof(value_type));
    _info->capacity = count > 100000 ? count : 100000;
    _info->size = count;
    _data = (value_type*) (_info + 1);
    for(iterator it=begin(); it!=end(); ++it)
      *it = value;
  }
  NVVector(io::NVVectorInfo* info) {
    _info = info;
    _data = (value_type*) (_info + 1);
  }
  NVVector(const NVVector& other) {
    _info = other._info;
    _data = other._data;
  }
  ~NVVector() {
    if(io::NVManager::getInstance().isInVolatileMode()) {
      if(_info->allocated > 0) io::NVManager::getInstance().destroyVectorSpace(_info);
    }
  }
  NVVector& operator=(const NVVector& other) = delete;
  NVVector& operator=(NVVector&& other) {
    // TODO: free (?) resources if vector was already initialized
    _info = other._info;
    _data = other._data;
  }

  void assign(size_type count, const_reference value) {
    for(size_type i = 0; i < _info->size; i++) _data[i] = value;
    resize(count, value);
  }
  // template <class InputIt>
  // void assign(InputIt first, InputIt last) {
    // TODO
  // }
  // void assign(std::initializer_list<T> ilist) {
    // TODO
  // }

  // element access
  reference at(size_type pos) {
    if(pos >= size()) {
      throw std::out_of_range();
    } else {
      return _data[pos];
    }
  }
  const_reference at(size_type pos) const {
    if(pos >= size()) {
      throw std::out_of_range();
    } else {
      return _data[pos];
    }
  }
  reference operator[](size_type pos) {
#if NVSIMULATOR_WRITE_NS > 0
    NVSimulator::write();
#endif
    assert(pos < size());
    return _data[pos];
  }
  const_reference operator[](size_type pos) const {
#if NVSIMULATOR_READ_NS > 0
    NVSimulator::read();
#endif
    assert(pos < size());
    return _data[pos];
  }
  reference front() {
    return _data[0];
  }
  const_reference front() const {
    return _data[0];
  }
  reference back() {
    return _data[size()-1];
  }
  const_reference back() const {
    return _data[size()-1];
  }
  value_type* data() {
    return _data;
  }
  const value_type* data() const {
    return _data;
  }

  // iterators
  iterator begin() {
    return _data;
  }
  const_iterator begin() const {
    return _data;
  }
  const_iterator cbegin() const {
    return _data;
  }
  iterator end() {
    return &_data[size()];
  }
  const_iterator end() const {
    return &_data[size()];
  }
  const_iterator cend() const {
    return &_data[size()];
  }

  // capacity
  bool empty() const {
    return size() == 0;
  }
  size_type size() const {
    return _info->size;
  }
  size_type max_size() const {
    return std::numeric_limits<size_type>::max();
  }
  void reserve(size_type new_cap) {
    if(new_cap <= capacity()) return;
    size_type oldSize = _info->size;
    _info = io::NVManager::getInstance().resizeVectorSpace(_info, new_cap*sizeof(value_type));
    _info->size = oldSize;
    _info->capacity = new_cap;
    _data = (value_type*) ((uintptr_t)_info + sizeof(io::NVVectorInfo));
  }
  size_type capacity() const {
    return _info->capacity;
  }
  void shrink_to_fit() {
    resize(size());
  }

  // modifiers
  void clear() {
    _info->size = 0;
  }
  iterator insert(const_iterator pos, const_reference value) {
    assert(size() < capacity()); // TODO: otherwise resize
    iterator it;
    for(it=_data[_info->size++]; it!=pos; --it)
      *it = *(it-1);
    *it = value;
    return it;
  }
  iterator insert(const_iterator pos, value_type&& value) {
    assert(size() < capacity()); // TODO: otherwise resize
    iterator it;
    for(it=_data[_info->size++]; it!=pos; --it)
      *it = *(it-1);
    *it = value;
    return it;
  }
  iterator insert(const_iterator pos, size_type count, const_reference value) {
    assert(size()+count <= capacity()); // TODO: otherwise resize
    iterator it;
    _info->size += count;
    for(it=_data[_info->size-1]; it!=(pos+(count-1)); --it)
      *it = *(it-count);
    for(; it!=pos; --it)
      *it = value;
    return it;
  }
  template <class InputIt>
  iterator insert(const_iterator pos, InputIt first, InputIt last ) {
    // TODO
  }
  iterator insert(const_iterator pos, std::initializer_list<T> ilist) {
    // TODO
  }
  iterator erase(const_iterator pos) {
    for(iterator it=pos; it!=&_data[--_info->size]; ++it)
      *it = *(it+1);
    return pos;
  }
  iterator erase(const_iterator first, const_iterator last) {
    difference_type count = last - first;
    iterator next = last+1;
    for(iterator it=first; it!=last && next!=end(); ++it)
      *it = *(next++);
    _info->size -= count;
    return first;
  }
  void push_back(const_reference value) {
    if(size() >= capacity()) {
      reserve(capacity()*2);
    }
    _data[_info->size++] = value;
  }
  void push_back(value_type&& value) {
    if(size() >= capacity()) {
      reserve(capacity()*2);
    }
    else _data[_info->size++] = value;
  }
  void pop_back() {
    --_info->size;
  }
  void resize(size_type count, const_reference value = value_type()) {
    if(count <= size()) {
      _info->size = count;
      return;
    }
    reserve(count);
    while(size() < count) {
      push_back(value);
    }
  }
  void swap(NVVector& other) {
    // TODO: tell NVManager about the swap to adjust metainfo
    uint64_t uuid1 = _info->uuid;
    uint64_t uuid2 = other._info->uuid;
    io::NVVectorInfo* info = other._info;
    value_type *data = other._data;
    other._info = _info;
    other._data = data;
    _info = info;
    _info->uuid = uuid1;
    other._info->uuid = uuid2;
  }

  // NVVector specific methods
  void persist(bool withoutData=false) {
    // persist only vector size, not capacity
    size_t len = withoutData ? sizeof(io::NVVectorInfo) : sizeof(io::NVVectorInfo) + _info->size*sizeof(value_type);
    io::NVManager::getInstance().persistMemoryArea(_info, len);
  }

  void persist_partial(size_type start, size_type num_elements = 1) {
    if(start == 0) {
      io::NVManager::getInstance().persistMemoryArea(_info, sizeof(io::NVVectorInfo) + num_elements*sizeof(value_type));
    } else {
      io::NVManager::getInstance().persistMemoryArea(_data, num_elements*sizeof(value_type));
      io::NVManager::getInstance().persistMemoryArea(_info, sizeof(io::NVVectorInfo));
    }
  }
  
  void persist_scattered(const std::vector<size_type>& elements, bool new_elements = true) const {
    // this function takes a scattered list of modified elements and persists them by cache line
    // consecutive cache lines are committed together.
    // Since PMEM_CHUNK_SIZE is 64, all vectors are automatically cache line-aligned.

    if(elements.size() == 0) return;

    size_type persist_start = 0, persist_length = 0;
    for(size_type i : elements) {
      size_type cache_line = i * sizeof(value_type) / 64;
      if(cache_line == persist_start + persist_length) {
        // consecutive cache lines
        persist_length++;
      } else {
        io::NVManager::getInstance().persistMemoryArea((void*)((uintptr_t)_data + persist_start * 64), persist_length * 64);
        persist_start = cache_line;
        persist_length = 1;
      }
    }
    io::NVManager::getInstance().persistMemoryArea((void*)((uintptr_t)_data + persist_start * 64), persist_length * 64);

    io::NVManager::getInstance().persistMemoryArea(_info, sizeof(io::NVVectorInfo));
  }

  uint64_t getUUID() const {
    return _info->uuid;
  }

private:
  io::NVVectorInfo* _info;
  value_type* _data;
};

}
}

#endif // SRC_LIB_STORAGE_NVVECTOR_H
#endif

