// Copyright (c) 2012 Hasso-Plattner-Institut fuer Softwaresystemtechnik GmbH. All rights reserved.
#pragma once

#include <memory>

#include <storage/BaseAttributeVector.h>
#include <storage/FixedLengthVector.h>
#include <storage/ConcurrentFixedLengthVector.h>
#include <storage/BitCompressedVector.h>
#ifdef PERSISTENCY_NVRAM
#include <storage/NVAttributeVector.h>
#endif

namespace hyrise {
namespace storage {

class AttributeVectorFactory {
public:

  template <typename T>
  static std::shared_ptr<BaseAttributeVector<T>> getAttributeVector(size_t columns = 1,
      size_t rows = 0,
      int distinct_values = 1,
      bool compressed = false,
      bool nonvolatile = false) {

#ifdef PERSISTENCY_NVRAM
    if(nonvolatile) {
      return std::make_shared<NVAttributeVector<T> >(columns, rows);
    } else
#endif
    {
      return std::make_shared<ConcurrentFixedLengthVector<T> >(columns, rows);
    }
  }

  template <typename T>
  static std::shared_ptr<BaseAttributeVector<T>> getAttributeVector2(size_t columns = 1,
      size_t rows = 0,
      bool compressed = false,
      std::vector<uint64_t> bits = std::vector<uint64_t> {},
      bool nonvolatile = false) {
    if (!compressed) {
#ifdef PERSISTENCY_NVRAM
      if(nonvolatile) {
        return std::make_shared<NVAttributeVector<T> >(columns, rows);
      } else
#endif
      {
        return std::make_shared<ConcurrentFixedLengthVector<T> >(columns, rows);
      }
    } else {
      return std::make_shared<BitCompressedVector<T> >(columns, rows, bits);
    }

  }
};

} } // namespace hyrise::storage

