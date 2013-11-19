// Copyright (c) 2013 Hasso-Plattner-Institut fuer Softwaresystemtechnik GmbH. All rights reserved.

#include "TpccDeliveryProcedure.h"

#include <storage/AbstractTable.h>
#include <access.h>
#include <unistd.h>

namespace hyrise { namespace access {

namespace {
  auto _ = net::Router::registerRoute<TpccDeliveryProcedure>("/TPCC-Delivery/");
} // namespace


TpccDeliveryProcedure::TpccDeliveryProcedure(net::AbstractConnection* connection) :
  TpccStoredProcedure(connection) {
}

void TpccDeliveryProcedure::setData(const Json::Value& data) {
  _w_id =         assureMemberExists(data, "W_ID").asInt();
  _o_carrier_id = assureIntValueBetween(data, "O_CARRIER_ID", 1, 10);
}

std::string TpccDeliveryProcedure::name() {
  return "TPCC-Delivery";
}

const std::string TpccDeliveryProcedure::vname() {
  return "TPCC-Delivery";
}

Json::Value TpccDeliveryProcedure::execute() {
  _date = getDate();

  Json::Value rows;

  storage::c_atable_ptr_t tNewOrder;
  for(int i = 0; i < 10; i++) {
    _d_id = i + 1;
    tNewOrder = getNewOrder();

    if (tNewOrder->size() == 0) continue;
    _o_id = tNewOrder->getValue<hyrise_int_t>("NO_O_ID", 0);

    auto tOrder = getCId();
    if (tOrder->size() == 0) {
      throw std::runtime_error("internal error: new order is associated with non-existing order");
    }
    _c_id = tOrder->getValue<hyrise_int_t>("O_C_ID", 0);

    auto tSum = sumOLAmount();
    if (tSum->size() == 0) {
      throw std::runtime_error("internal error: no order lines for existing order");
    }
    _total = tSum->getValue<hyrise_float_t>(0, 0);

    deleteNewOrder();
    updateOrders(std::const_pointer_cast<AbstractTable>(tOrder));
    updateOrderLine();
    updateCustomer();

    Json::Value row;
    row[0] = _w_id;
    row[1] = _o_id;
    rows.append(row);
  }

  commit();

  Json::Value result;
  Json::Value header;
  header[0] = "W_ID";
  header[1] = "NO_O_ID";
  result["header"] = header;
  result["rows"] = rows;
  result["Execution Status"] = "Delivery has been queued";

  return result;
}

void TpccDeliveryProcedure::deleteNewOrder() {
  auto newOrder = getTpccTable("NEW_ORDER");

  expr_list_t expressions;
  expressions.push_back(new GenericExpressionValue<hyrise_int_t, std::equal_to<hyrise_int_t>>(newOrder->getDeltaTable(), "NO_W_ID", _w_id));
  expressions.push_back(new GenericExpressionValue<hyrise_int_t, std::equal_to<hyrise_int_t>>(newOrder->getDeltaTable(), "NO_D_ID", _d_id));
  expressions.push_back(new GenericExpressionValue<hyrise_int_t, std::equal_to<hyrise_int_t>>(newOrder->getDeltaTable(), "NO_O_ID", _o_id));
  auto validated = selectAndValidate(newOrder, "NEW_ORDER", connectAnd(expressions));

  deleteRows(validated);
}

storage::c_atable_ptr_t TpccDeliveryProcedure::getCId() {
  auto orders = getTpccTable("ORDERS");

  expr_list_t expressions;
  expressions.push_back(new GenericExpressionValue<hyrise_int_t, std::equal_to<hyrise_int_t>>(orders->getDeltaTable(), "O_W_ID", _w_id));
  expressions.push_back(new GenericExpressionValue<hyrise_int_t, std::equal_to<hyrise_int_t>>(orders->getDeltaTable(), "O_D_ID", _d_id));
  expressions.push_back(new GenericExpressionValue<hyrise_int_t, std::equal_to<hyrise_int_t>>(orders->getDeltaTable(), "O_ID", _o_id));
  auto validated = selectAndValidate(orders, "ORDERS", connectAnd(expressions));

  return validated;
}

storage::c_atable_ptr_t TpccDeliveryProcedure::getNewOrder() {
  auto newOrder = getTpccTable("NEW_ORDER");

  expr_list_t expressions;
  expressions.push_back(new GenericExpressionValue<hyrise_int_t, std::equal_to<hyrise_int_t>>(newOrder->getDeltaTable(), "NO_W_ID", _w_id));
  expressions.push_back(new GenericExpressionValue<hyrise_int_t, std::equal_to<hyrise_int_t>>(newOrder->getDeltaTable(), "NO_D_ID", _d_id));
  auto validated = selectAndValidate(newOrder, "NEW_ORDER", connectAnd(expressions));

  auto sorted = sort(validated, "NO_O_ID", true);
  return sorted;
}

storage::c_atable_ptr_t TpccDeliveryProcedure::sumOLAmount() {
  auto orderLine = getTpccTable("ORDER_LINE");

  expr_list_t expressions;
  expressions.push_back(new GenericExpressionValue<hyrise_int_t, std::equal_to<hyrise_int_t>>(orderLine->getDeltaTable(), "OL_W_ID", _w_id));
  expressions.push_back(new GenericExpressionValue<hyrise_int_t, std::equal_to<hyrise_int_t>>(orderLine->getDeltaTable(), "OL_D_ID", _d_id));
  expressions.push_back(new GenericExpressionValue<hyrise_int_t, std::equal_to<hyrise_int_t>>(orderLine->getDeltaTable(), "OL_O_ID", _o_id));
  auto validated = selectAndValidate(orderLine, "ORDER_LINE", connectAnd(expressions));

  std::shared_ptr<GroupByScan> groupby = std::make_shared<GroupByScan>();
  groupby->setOperatorId("__GroupByScan");
  groupby->setPlanOperationName("GroupByScan");
  _responseTask->registerPlanOperation(groupby);

  groupby->addInput(validated);
  auto sum = new SumAggregateFun("OL_AMOUNT");
  groupby->addFunction(sum);
  groupby->execute();

  return groupby->getResultTable();
}

void TpccDeliveryProcedure::updateCustomer() {

  auto customer = getTpccTable("CUSTOMER");

  expr_list_t expressions;
  expressions.push_back(new GenericExpressionValue<hyrise_int_t, std::equal_to<hyrise_int_t>>(customer->getDeltaTable(), "C_ID", _c_id));
  expressions.push_back(new GenericExpressionValue<hyrise_int_t, std::equal_to<hyrise_int_t>>(customer->getDeltaTable(), "C_W_ID", _w_id));
  expressions.push_back(new GenericExpressionValue<hyrise_int_t, std::equal_to<hyrise_int_t>>(customer->getDeltaTable(), "C_D_ID", _d_id));
  auto validated = selectAndValidate(customer, "CUSTOMER", connectAnd(expressions));

  Json::Value updates;
  updates["C_BALANCE"] = _total;
  update(validated, updates);
}

void TpccDeliveryProcedure::updateOrderLine() {
  auto orderLine = getTpccTable("ORDER_LINE");

  expr_list_t expressions;
  expressions.push_back(new GenericExpressionValue<hyrise_int_t, std::equal_to<hyrise_int_t>>(orderLine->getDeltaTable(), "OL_W_ID", _w_id));
  expressions.push_back(new GenericExpressionValue<hyrise_int_t, std::equal_to<hyrise_int_t>>(orderLine->getDeltaTable(), "OL_D_ID", _d_id));
  expressions.push_back(new GenericExpressionValue<hyrise_int_t, std::equal_to<hyrise_int_t>>(orderLine->getDeltaTable(), "OL_O_ID", _o_id));
  auto validated = selectAndValidate(orderLine, "ORDER_LINE", connectAnd(expressions));

  Json::Value updates;
  updates["OL_DELIVERY_D"] = _date;
  update(validated, updates);
}

void TpccDeliveryProcedure::updateOrders(const storage::atable_ptr_t& ordersRow) {
  Json::Value updates;
  updates["O_CARRIER_ID"] = _o_carrier_id;
  update(ordersRow, updates);
}


}} // namespace hyrise::access

