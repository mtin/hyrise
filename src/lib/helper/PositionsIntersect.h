// Copyright (c) 2012 Hasso-Plattner-Institut fuer Softwaresystemtechnik GmbH. All rights reserved.
#ifndef SRC_LIB_HELPER_POSITIONSINTERESECT_H_
#define SRC_LIB_HELPER_POSITIONSINTERESECT_H_

#include <storage/storage_types.h>

// produces sorted intersection of two pos lists. based on
// baeza-yates algorithm with average complexity nicely
// adapting to the smaller list size. whereas std::set_intersection
// iterates over both lists with linear complexity.
void intersect_pos_list(  pos_list_t::iterator beg1, pos_list_t::iterator end1,
                          pos_list_t::iterator beg2, pos_list_t::iterator end2,
                          pos_list_t *result,
                          bool first_sorted=true, bool second_sorted=true);

#endif

