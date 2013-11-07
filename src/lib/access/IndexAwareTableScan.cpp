// Copyright (c) 2012 Hasso-Plattner-Institut fuer Softwaresystemtechnik GmbH. All rights reserved.
#include "access/IndexAwareTableScan.h"

#include <memory>

#include "access/system/BasicParser.h"
#include "access/json_converters.h"
#include "access/system/QueryParser.h"
#include "access/expressions/pred_buildExpression.h"

#include "io/StorageManager.h"

#include "storage/InvertedIndex.h"
#include "storage/Store.h"
#include "storage/meta_storage.h"
#include "storage/PointerCalculator.h"

#include "helper/checked_cast.h"

#include "access/expressions/pred_buildExpression.h"
#include "access/expressions/expression_types.h"
#include "access/IntersectPositions.h"
#include "access/SimpleTableScan.h"

namespace hyrise {
namespace access {

namespace {
  auto _ = QueryParser::registerPlanOperation<IndexAwareTableScan>("IndexAwareTableScan");
}

IndexAwareTableScan::IndexAwareTableScan() { }

IndexAwareTableScan::~IndexAwareTableScan() {
  for (auto _idx_scan : _is ) delete _idx_scan;
  // predicate gets deleted by SimpleTableScan
}

void IndexAwareTableScan::executePlanOperation() {

  SimpleTableScan _ts;
  
  // disable papi trace as nested operators breaks it
  _ts.disablePapiTrace();

  // Prepare Index Scan for the Main
  storage::c_atable_ptr_t t = input.getTables()[0];
  for (auto _idx_scan : _is ) {
    _idx_scan->disablePapiTrace();
    _idx_scan->addInput(t);
  }
  
  // Preapare Table Scan for the Delta
  _ts.setPredicate(_predicate);
  _ts.setProducesPositions(true);
  auto t_store = std::dynamic_pointer_cast<const storage::Store>(t);
  if(t_store) {
    _ts.addInput(t_store->getDeltaTable());
  } else {
    _ts.addInput(t);      
  }

  for (auto _idx_scan : _is )
    _idx_scan->execute();
  _ts.execute();

  std::shared_ptr<const PointerCalculator> pc1;
  if (_is.size() > 1) {
    // intersect positions of all index scans
    IntersectPositions intersect;
    intersect.disablePapiTrace();
    for (auto _idx_scan : _is ) {
      intersect.addInput(_idx_scan->getResultTable());
    }
    intersect.execute();
    pc1 = checked_pointer_cast<const PointerCalculator>(intersect.getResultTable());
  } else {
    pc1 = checked_pointer_cast<const PointerCalculator>(_is[0]->getResultTable());
  }
  
  auto pc2 = checked_pointer_cast<const PointerCalculator>(_ts.getResultTable());

  if(t_store) {
    pos_list_t pos = *pc2->getPositions();
    pos_t offset = t_store->getMainTables()[0]->size();

    std::for_each(pos.begin(), pos.end(), [&offset](pos_t& d) { d += offset;});

    std::shared_ptr<PointerCalculator> pc2_absolute(PointerCalculator::create(t));
    pc2_absolute->setPositions(pos);
    addResult(pc1->concatenate(pc2_absolute));
  } else {
    addResult(pc1->concatenate(pc2));
  }
}

struct CreateIndexValueFunctor {
  typedef AbstractIndexValue *value_type;

  const Json::Value &_d;

  explicit CreateIndexValueFunctor(const Json::Value &c): _d(c) {}

  template<typename R>
  value_type operator()() {
    IndexValue<R> *v = new IndexValue<R>();
    v->value = json_converter::convert<R>(_d["value"]);
    return v;
  }
};

std::shared_ptr<PlanOperation> IndexAwareTableScan::parse(const Json::Value &data) {

  std::shared_ptr<IndexAwareTableScan> idx_scan = BasicParser<IndexAwareTableScan>::parse(data);

  if (!data.isMember("predicates")) {
    throw std::runtime_error("There is no reason for a IndexAwareScan without predicates");
  }
  if (!data.isMember("tablename")) {
    throw std::runtime_error("IndexAwareScan needs a base table name");
  }

  idx_scan->setPredicate(buildExpression(data["predicates"]));
  idx_scan->setTablename(data["tablename"].asString());

  Json::Value predicate;
  PredicateType::type pred_type;
  std::string tablename = data["tablename"].asString();

  for (unsigned i = 0; i < data["predicates"].size(); ++i) {
    predicate = data["predicates"][i];
    pred_type = parsePredicateType(predicate["type"]);

    if (pred_type == PredicateType::AND) continue;

    if (pred_type != PredicateType::EqualsExpressionValue && 
        pred_type != PredicateType::LessThanExpressionValue && 
        pred_type != PredicateType::GreaterThanExpressionValue &&
        pred_type != PredicateType::LessThanEqualsExpressionValue &&
        pred_type != PredicateType::GreaterThanEqualsExpressionValue)
      throw std::runtime_error("IndexAwareScan: Unsupported predicate type");
    
    if (predicate["f"].isNumeric())
        throw std::runtime_error("For now, IndexAwareScan requires fields to be specified via fieldnames");
    IndexScan *s = new IndexScan;
    storage::type_switch<hyrise_basic_types> ts;
    CreateIndexValueFunctor civf(predicate);
    s->_value1 = ts(predicate["vtype"].asUInt(), civf);
    s->setIndexName("idx__"+tablename+"__"+predicate["f"].asString());
    s->addField(predicate["f"].asString());
    s->setPredicateType(pred_type);
    idx_scan->addIndexScan(s);
  }

  return idx_scan;
}

const std::string IndexAwareTableScan::vname() {
  return "IndexAwareTableScan";
}

void IndexAwareTableScan::setPredicate(SimpleExpression *c) {
  _predicate = c;
}

void IndexAwareTableScan::setTablename(std::string name) {
  _tablename = name;
}

void IndexAwareTableScan::addIndexScan(IndexScan* s) {
  _is.push_back(s);
}

}
}

