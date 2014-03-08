// Copyright (c) 2014 Hasso-Plattner-Institut fuer Softwaresystemtechnik GmbH. All rights reserved.
#ifndef SRC_LIB_STORAGE_PAGEDINDEXMERGER_H_
#define SRC_LIB_STORAGE_PAGEDINDEXMERGER_H_

#include "AbstractMerger.h"

namespace hyrise { namespace storage {

class PagedIndexMerger : public hyrise::storage::AbstractMerger {

public:
	PagedIndexMerger(std::string index_name);

 	virtual ~PagedIndexMerger(){}

	void mergeValues(const std::vector<c_atable_ptr_t> &input_tables,
                   atable_ptr_t merged_table,
                   const column_mapping_t &column_mapping,
                   const uint64_t newSize,
                   bool useValid = false,
                   const std::vector<bool>& valid = std::vector<bool>());
  
	AbstractMerger* copy() { return nullptr; }

private:
  	const std::string _index_name;

};

}}

#endif /* SRC_LIB_STORAGE_PAGEDINDEXMERGER_H_ */