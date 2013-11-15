// Copyright (c) 2013 Hasso-Plattner-Institut fuer Softwaresystemtechnik GmbH. All rights reserved.

#include "TpccStockLevelProcedure.h"

#include <storage/AbstractTable.h>
#include <access.h>

namespace hyrise { namespace access {

namespace {
  auto _ = net::Router::registerRoute<TpccStockLevelProcedure>("/TPCC-StockLevel/");
} // namespace


TpccStockLevelProcedure::TpccStockLevelProcedure(net::AbstractConnection* connection) :
  TpccStoredProcedure(connection) {
}

void TpccStockLevelProcedure::setData(const Json::Value& data) {
  _w_id = assureMemberExists(data, "W_ID").asInt();
  _d_id = assureIntValueBetween(data, "D_ID", 1, 10);
  _threshold = assureIntValueBetween(data, "threshold", 10, 20);
}

std::string TpccStockLevelProcedure::name() {
  return "TPCC-StockLevel";
}

const std::string TpccStockLevelProcedure::vname() {
  return "TPCC-StockLevel";
}

Json::Value TpccStockLevelProcedure::execute() {
  auto t1 = getOId();
  if (t1->size() == 0) {
    std::ostringstream os;
    os << "no district " << _d_id << " for warehouse " << _w_id;
    throw std::runtime_error(os.str());
  }
  _next_o_id = t1->getValue<hyrise_int_t>("D_NEXT_O_ID", 0);

  auto t2 = getStockCount();
  commit();

  // Output
  int low_stock = 0;
  if (t2->size() != 0) 
    low_stock = t2->getValue<hyrise_int_t>(0, 0);

  Json::Value result;
  result["W_ID"] = _w_id;
  result["D_ID"] = _d_id;
  result["threshold"] = _threshold;
  result["low_stock"] = low_stock;
  return result;
}

storage::c_atable_ptr_t TpccStockLevelProcedure::getOId() {
  auto district = getTpccTable("DISTRICT");

  expr_list_t expressions;
  expressions.push_back(new GenericExpressionValue<hyrise_int_t, std::equal_to<hyrise_int_t>>(district->getDeltaTable(), "D_W_ID", _w_id));
  expressions.push_back(new GenericExpressionValue<hyrise_int_t, std::equal_to<hyrise_int_t>>(district->getDeltaTable(), "D_ID", _d_id));
  auto validated = selectAndValidate(district, "DISTRICT", connectAnd(expressions));

  return validated;
}

storage::c_atable_ptr_t TpccStockLevelProcedure::getStockCount() {
  // order_line: load, select and validate
  auto order_line = getTpccTable("ORDER_LINE");
  const auto min_o_id = _next_o_id - 21;
  const auto max_o_id = _next_o_id + 1; //D_NEXT_O_ID shoult be greater than every O_ID

  expr_list_t expressions_ol;
  expressions_ol.push_back(new GenericExpressionValue<hyrise_int_t, std::equal_to<hyrise_int_t>>(order_line->getDeltaTable(), "OL_W_ID", _w_id));
  expressions_ol.push_back(new GenericExpressionValue<hyrise_int_t, std::equal_to<hyrise_int_t>>(order_line->getDeltaTable(), "OL_D_ID", _d_id));
  expressions_ol.push_back(new GenericExpressionValue<hyrise_int_t, std::less<hyrise_int_t>>(order_line->getDeltaTable(), "OL_O_ID", max_o_id));
  expressions_ol.push_back(new GenericExpressionValue<hyrise_int_t, std::greater<hyrise_int_t>>(order_line->getDeltaTable(), "OL_O_ID", min_o_id));
  auto validated_ol = selectAndValidate(order_line, "ORDER_LINE", connectAnd(expressions_ol));

  auto stock = getTpccTable("STOCK");
  expr_list_t expressions_s;
  expressions_s.push_back(new GenericExpressionValue<hyrise_int_t, std::equal_to<hyrise_int_t>>(stock->getDeltaTable(), "S_W_ID", _w_id));
  expressions_s.push_back(new GenericExpressionValue<hyrise_int_t, std::less<hyrise_int_t>>(stock->getDeltaTable(), "S_QUANTITY", _threshold));
  auto validated_s = selectAndValidate(stock, "STOCK", connectAnd(expressions_s));


  std::shared_ptr<HashBuild> hb = std::make_shared<HashBuild>();
  hb->setOperatorId("__HashBuild");
  hb->setPlanOperationName("HashBuild");
  _responseTask->registerPlanOperation(hb);
  hb->addInput(validated_ol);
  hb->addField("OL_I_ID");
  hb->setKey("join");
  hb->execute();

  std::shared_ptr<HashJoinProbe> hjp = std::make_shared<HashJoinProbe>();
  hjp->setOperatorId("__HashJoinProbe");
  hjp->setPlanOperationName("HashJoinProbe");
  _responseTask->registerPlanOperation(hjp);
  hjp->addInput(validated_s);
  hjp->addField("S_I_ID");
  hjp->addInput(hb->getResultHashTable());
  hjp->execute();


  std::shared_ptr<GroupByScan> groupby = std::make_shared<GroupByScan>();
  groupby->setOperatorId("__GroupByScan");
  groupby->setPlanOperationName("GroupByScan");
  _responseTask->registerPlanOperation(groupby);
  groupby->addInput(hjp->getResultTable());
  auto count = new CountAggregateFun("OL_I_ID");
  count->setDistinct(true);
  groupby->addFunction(count);
  groupby->execute();

  return groupby->getResultTable();
}

} } // namespace hyrise::access

