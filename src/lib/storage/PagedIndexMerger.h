// Copyright (c) 2014 Hasso-Plattner-Institut fuer Softwaresystemtechnik GmbH. All rights reserved.
#ifndef SRC_LIB_STORAGE_PAGEDINDEXMERGER_H_
#define SRC_LIB_STORAGE_PAGEDINDEXMERGER_H_

#include "AbstractMerger.h"

namespace hyrise { namespace storage {

class PagedIndexMerger : public hyrise::storage::AbstractMerger {

public:
	PagedIndexMerger(const std::string index_name, const std::string delta_name, field_t column);

 	virtual ~PagedIndexMerger(){}

	void mergeValues(const std::vector<c_atable_ptr_t> &input_tables,
                   atable_ptr_t merged_table,
                   const column_mapping_t &column_mapping,
                   const uint64_t newSize,
                   bool useValid = false,
                   const std::vector<bool>& valid = std::vector<bool>());
  
	AbstractMerger* copy() { return nullptr; }

private:
  	std::string _index_name;
  	std::string _delta_name;
  	field_t		_column;

};

}}

#endif /* SRC_LIB_STORAGE_PAGEDINDEXMERGER_H_ */