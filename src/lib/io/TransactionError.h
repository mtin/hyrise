// Copyright (c) 2012 Hasso-Plattner-Institut fuer Softwaresystemtechnik GmbH. All rights reserved.
#ifndef SRC_LIB_IO_TRANSACTIONERROR_H_
#define SRC_LIB_IO_TRANSACTIONERROR_H_

namespace hyrise {
namespace tx {

class transaction_error : public std::runtime_error {
 	using std::runtime_error::runtime_error;
};

}}

#endif