// Copyright (c) 2012 Hasso-Plattner-Institut fuer Softwaresystemtechnik GmbH. All rights reserved.
#ifndef SRC_LIB_HELPER_assembled_container_H_
#define SRC_LIB_HELPER_assembled_container_H_

#include <iostream>
#include <stdexcept>

// A meta container that can add multiple containers and provides an iterator
// to iterate over these containers while abstracting the factthat they are
// multiple individual containers. The meta container only stores iterators
// and has no ownership of the undelying containers. Access is read only, so
// that the underlying containers cannot be modified.

template<class ContainerType, class ValueType>
class AssembledContainer
{ 
private:
  typedef ContainerType container_t;
  typedef typename container_t::const_iterator container_it_t;
  typedef typename std::vector<std::pair<container_it_t, container_it_t> > meta_container_t;
  typedef typename meta_container_t::const_iterator meta_container_it_t;

  meta_container_t _meta_container;
  bool _is_sorted = false;
  size_t _size = 0;

public:
  class iterator : public std::iterator<std::forward_iterator_tag, ValueType>
  {
  private:
    size_t _pos = 0;
    container_it_t _current;
    meta_container_it_t _current_container;
    const AssembledContainer *_assembled_container;

    void _reset() {
      _current_container = _assembled_container->_meta_container.cbegin();
      _pos = 0;
    }

    bool _is_at_end() {
      return _current_container == _assembled_container->_meta_container.end();
    }

    bool _is_at_container_end() {
      return _current == _current_container->second;
    }

    size_t _current_size() {
      return _current_container->second - _current_container->first;
    }

    void _update_current(size_t container_pos) {
      if (_current_container != _assembled_container->_meta_container.end())
        _current = _current_container->first + container_pos;
    }
    void _next_container() {
      ++_current_container;
    }

    void _next() {
      _current++;
      _pos++;
      if ( _is_at_container_end() ) {
        _next_container();
        _update_current(0);
      }
    }

    void _set_position(size_t pos) {
      _reset();
      while ( !_is_at_end() && _pos + _current_size() <= pos ) {
        _pos += _current_size();
        _next_container();
      }
      _update_current(pos - _pos);
      _pos = pos;
    }

  public:

    iterator(const AssembledContainer *assembled_container, size_t pos=0) {
      _assembled_container = assembled_container;
      _set_position(pos);
    };

    const ValueType& operator*() const {
      return *_current;
    }

    const ValueType* operator->() const {
      return *_current;
    }

    iterator& operator++(){
      _next();
      return *this;
    };

    bool operator==(const iterator &other) const {
      return _pos == other._pos;
    }

    bool operator!=(const iterator &other) const {
      return _pos != other._pos;
    }

    iterator& operator+=(const size_t rhs)
    {
      _set_position(_pos + rhs);
      return *this;
    }

    iterator& operator-=(const size_t rhs)
    {
      _set_position(_pos - rhs);
      return *this;
    }

    iterator operator-(size_t rhs) const
    {
      auto result = *this;
      result._set_position(_pos - rhs);
      return result;
    }

    iterator operator+(size_t rhs) const
    {
      auto result = *this;
      result._set_position(_pos + rhs);
      return result;
    }

    size_t operator-(iterator &other) const
    {
      return _pos - other._pos;
    }

    friend AssembledContainer;
  };

  AssembledContainer() = default;

  // Create a new AssembledContainer with one container specified through begin and end
  AssembledContainer(container_it_t begin, container_it_t end, bool is_sorted)
   : _is_sorted(is_sorted) {
    add(begin, end);
  }

  // Add a new container to the meta container
  void add(container_it_t begin, container_it_t end) {
    auto size = end - begin;
    if(size > 0) {
      _meta_container.push_back(std::make_pair(begin, end));
      _size += size;
    }
  }

  iterator begin() const {
    return iterator(this, 0);
  }

  iterator end() const {
    return iterator(this, _size);
  }

  // Copy all managed containers into 'out'. Calling copiInto allows for better
  // optimizations using memcpy instead of copying every single element throug the iterator.
  void copyInto(container_t &out) {
    out.reserve(_size);
    for (auto pair : _meta_container)
      out.insert(out.end(), pair.first, pair.second);
  }

  bool isSorted() const {
    return _is_sorted;
  }

  size_t size() const {
    return _size;
  }
};



#endif // SRC_LIB_HELPER_assembled_container_H_
