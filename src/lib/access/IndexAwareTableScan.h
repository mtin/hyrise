// Copyright (c) 2012 Hasso-Plattner-Institut fuer Softwaresystemtechnik GmbH. All rights reserved.
#ifndef SRC_LIB_ACCESS_INDEXAWARETABLESCAN_SCAN
#define SRC_LIB_ACCESS_INDEXAWARETABLESCAN_SCAN

#include "access/system/PlanOperation.h"
#include "access/IndexScan.h"
#include "access/TableScan.h"
#include "access/expressions/pred_EqualsExpression.h"

namespace hyrise {
namespace access {

struct GroupkeyIndexFunctor;

/// Scan an existing index for the result. Currently only EQ predicates
/// allowed for the index.
class IndexAwareTableScan : public PlanOperation {
public:
  explicit IndexAwareTableScan();
  void executePlanOperation();
  ~IndexAwareTableScan();
  static std::shared_ptr<PlanOperation> parse(const Json::Value &data);
  const std::string vname();
  void setPredicate(AbstractExpression *c);
  void setTableName(std::string name);
  
private:
  void _setPredicateFromJson(AbstractExpression *c);
  SimpleExpression *_predicate;
  std::vector<GroupkeyIndexFunctor> _idx_functors;
  std::string _tableName;
};

}
}

#endif // SRC_LIB_ACCESS_INDEXAWARETABLESCAN_SCAN
