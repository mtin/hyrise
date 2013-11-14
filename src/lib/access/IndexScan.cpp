// Copyright (c) 2012 Hasso-Plattner-Institut fuer Softwaresystemtechnik GmbH. All rights reserved.
#include "access/IndexScan.h"

#include <memory>

#include "access/system/BasicParser.h"
#include "access/json_converters.h"
#include "access/system/QueryParser.h"

#include "io/StorageManager.h"

#include "storage/InvertedIndex.h"
#include "storage/meta_storage.h"
#include "storage/PointerCalculator.h"

namespace hyrise {
namespace access {

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

struct ScanIndexFunctor {
  typedef storage::pos_list_t *value_type;

  std::shared_ptr<AbstractIndex> _index;
  AbstractIndexValue *_indexValue1;
  AbstractIndexValue *_indexValue2;
  PredicateType::type _predicate_type;

  ScanIndexFunctor(AbstractIndexValue *indexValue1, AbstractIndexValue *indexValue2, std::shared_ptr<AbstractIndex> d, PredicateType::type predicate_type):
    _index(d), _indexValue1(indexValue1), _indexValue2(indexValue2), _predicate_type(predicate_type) {}

  template<typename ValueType>
  value_type operator()() {
    auto idx = std::dynamic_pointer_cast<InvertedIndex<ValueType>>(_index);
    auto v1 = static_cast<IndexValue<ValueType>*>(_indexValue1);
    auto v2 = static_cast<IndexValue<ValueType>*>(_indexValue2);
    storage::pos_list_t *result = nullptr;

    switch (_predicate_type) {
      case PredicateType::EqualsExpression:
      case PredicateType::EqualsExpressionValue:
        result = new storage::pos_list_t(idx->getPositionsForKey(v1->value));
        break;
      case PredicateType::GreaterThanExpression:
      case PredicateType::GreaterThanExpressionValue:
        result = new storage::pos_list_t(idx->getPositionsForKeyGT(v1->value));
        break;
      // case PredicateType::GreaterThanEqualsExpression:
      //   result = new storage::pos_list_t(idx->getPositionsForKeyGTE(v1->value));
      //   break;
      case PredicateType::LessThanExpression:
      case PredicateType::LessThanExpressionValue:
        result = new storage::pos_list_t(idx->getPositionsForKeyLT(v1->value));
        break;
      // case PredicateType::LessThanEqualsExpression:
      //   result = new storage::pos_list_t(idx->getPositionsForKeyLTE(v1->value));
      //   break;
      case PredicateType::BetweenExpression:
        result = new storage::pos_list_t(idx->getPositionsForKeyBetween(v1->value, v2->value));
        break;
      default:
        throw std::runtime_error("Unsupported predicate type in IndexScan");
    }
    return result;
  }
};

namespace {
  auto _ = QueryParser::registerPlanOperation<IndexScan>("IndexScan");
}


IndexScan::IndexScan() :
  _value1(nullptr), _value2(nullptr) {}

IndexScan::~IndexScan() {
  if (_value1 != nullptr) delete _value1; 
  if (_value2 != nullptr) delete _value2;
}

void IndexScan::executePlanOperation() {
  StorageManager *sm = StorageManager::getInstance();
  auto idx = sm->getInvertedIndex(_indexName);

  // Handle type of index and value
  storage::type_switch<hyrise_basic_types> ts;
  ScanIndexFunctor fun(_value1, _value2, idx, _predicate_type);
  storage::pos_list_t *pos = ts(input.getTable(0)->typeOfColumn(_field_definition[0]), fun);

  addResult(PointerCalculator::create(input.getTable(0), pos));
}

std::shared_ptr<PlanOperation> IndexScan::parse(const Json::Value &data) {


  std::shared_ptr<IndexScan> s = BasicParser<IndexScan>::parse(data);
  storage::type_switch<hyrise_basic_types> ts;
  CreateIndexValueFunctor civf(data);
  s->_value1 = ts(data["vtype"].asUInt(), civf);
  s->_indexName = data["index"].asString();
  // if (data.isMember("predicate_type"))
  //   s->setPredicateType(parsePredicateType(predicate["predicate_type"]));
  // else
  s->setPredicateType(parsePredicateType(PredicateType::EqualsExpression));
  return s;
}

const std::string IndexScan::vname() {
  return "IndexScan";
}

void IndexScan::setIndexName(const std::string &name) {
  _indexName = name;
}

namespace {
  auto _2 = QueryParser::registerPlanOperation<MergeIndexScan>("MergeIndexScan");
}

MergeIndexScan::~MergeIndexScan() {
}

void MergeIndexScan::executePlanOperation() {
  auto left = std::dynamic_pointer_cast<const PointerCalculator>(input.getTable(0));
  auto right = std::dynamic_pointer_cast<const PointerCalculator>(input.getTable(1));

  storage::pos_list_t result(std::max(left->getPositions()->size(), right->getPositions()->size()));

  auto it = std::set_intersection(left->getPositions()->begin(),
                                  left->getPositions()->end(),
                                  right->getPositions()->begin(),
                                  right->getPositions()->end(),
                                  result.begin());

  auto tmp = PointerCalculator::create(left->getActualTable(), new storage::pos_list_t(result.begin(), it));
  addResult(tmp);
}

std::shared_ptr<PlanOperation> MergeIndexScan::parse(const Json::Value &data) {
  return BasicParser<MergeIndexScan>::parse(data);
}

const std::string MergeIndexScan::vname() {
  return "MergeIndexScan";
}

}
}
