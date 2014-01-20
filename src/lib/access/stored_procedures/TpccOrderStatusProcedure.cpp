// Copyright (c) 2013 Hasso-Plattner-Institut fuer Softwaresystemtechnik GmbH. All rights reserved.

#include "TpccOrderStatusProcedure.h"

#include <storage/AbstractTable.h>
#include <access.h>

namespace hyrise { namespace access {

namespace {
  auto _ = net::Router::registerRoute<TpccOrderStatusProcedure>("/TPCC-OrderStatus/");
} // namespace


TpccOrderStatusProcedure::TpccOrderStatusProcedure(net::AbstractConnection* connection) :
  TpccStoredProcedure(connection) {
}

void TpccOrderStatusProcedure::setData(const Json::Value& data) {
  _w_id = assureMemberExists(data, "W_ID").asInt();
  _d_id = assureIntValueBetween(data, "D_ID", 1, 10);

  const bool id = data["C_ID"].isInt();
  const bool last = data["C_LAST"].isString();
  if (id && last)
    throw std::runtime_error("Customer selection via either C_ID or C_LAST - both are provided");
  else if (!id && !last)
    throw std::runtime_error("Customer selection via either C_ID or C_LAST - provide one of them");
  else if (id) {
    _customerById = true;
    _c_id = data["C_ID"].asInt();
  }
  else {
    _customerById = false;
    _c_last = data["C_LAST"].asString();
  }
}

std::string TpccOrderStatusProcedure::name() {
  return "TPCC-OrderStatus";
}

const std::string TpccOrderStatusProcedure::vname() {
  return "TPCC-OrderStatus";
}

Json::Value TpccOrderStatusProcedure::execute() {
  storage::atable_ptr_t tCustomer;
  if (_customerById) {
    tCustomer = std::const_pointer_cast<storage::AbstractTable>(getCustomerByCId());
    if (tCustomer->size() == 0) {
      rollback();
      std::ostringstream os;
      os << "No Customer with ID: " << _c_id;
      throw std::runtime_error(os.str());
    }

    _chosenOne = 0;
    _c_last = tCustomer->getValue<hyrise_string_t>("C_LAST", 0);
  }
  else {
    tCustomer = std::const_pointer_cast<storage::AbstractTable>(getCustomersByLastName());
    if (tCustomer->size() == 0) {
      rollback();
      throw std::runtime_error("No Customer with ID: " + _c_last);
    }

    _chosenOne = tCustomer->size() / 2;
    _c_id = tCustomer->getValue<hyrise_int_t>("C_ID", _chosenOne);
  }

  auto tOrders = getLastOrder();
  if (tOrders->size() == 0) {
    std::ostringstream os;
    os << "no active order for customer " << _c_id << " in district " << _d_id << " of warehouse " << _w_id;
    throw std::runtime_error(os.str());
  }
  _o_id = tOrders->getValue<hyrise_int_t>("O_ID", 0);
  const std::string o_entry_d = tOrders->getValue<hyrise_string_t>("O_ENTRY_D", 0);
  const int o_carrier_id = tOrders->getValue<hyrise_int_t>("O_CARRIER_ID", 0);

  auto tOrderLines = getOrderLines();
  if (tOrderLines->size() < 5 || tOrderLines->size() > 15)
    throw std::runtime_error("internal error: there must be between 5 and 15 orderlines for every order");

  // commit();

  // Output
  Json::Value result;
  result["W_ID"]         = _w_id;
  result["D_ID"]         = _d_id;
  result["C_ID"]         = _c_id;
  result["C_FIRST"]      = tCustomer->getValue<hyrise_string_t>("C_FIRST", _chosenOne);
  result["C_MIDDLE"]     = tCustomer->getValue<hyrise_string_t>("C_MIDDLE", _chosenOne);
  result["C_LAST"]       = _c_last;
  result["C_BALANCE"]    = tCustomer->getValue<hyrise_float_t>("C_BALANCE", _chosenOne);
  result["O_ID"]         = _o_id;
  result["O_ENTRY_D"]    = _date;
  result["O_CARRIER_ID"] = o_carrier_id;

  Json::Value orderLines(Json::arrayValue);
  for (size_t i = 0; i < tOrderLines->size(); ++i) {
    Json::Value orderLine;
    orderLine["OL_SUPPLY_W_ID"] = tOrderLines->getValue<hyrise_int_t>("OL_SUPPLY_W_ID", i);
    orderLine["OL_I_ID"]        = tOrderLines->getValue<hyrise_int_t>("OL_I_ID", i);
    orderLine["OL_QUANTITY"]    = tOrderLines->getValue<hyrise_int_t>("OL_QUANTITY", i);
    orderLine["OL_AMOUNT"]      = tOrderLines->getValue<hyrise_float_t>("OL_AMOUNT", i);
    orderLine["OL_DELIVERY_D"]  = tOrderLines->getValue<hyrise_string_t>("OL_DELIVERY_D", i);
    orderLines.append(orderLine);
  }
  result["order lines"] = orderLines;

  return result;
}

storage::c_atable_ptr_t TpccOrderStatusProcedure::getCustomerByCId() {
  auto customer = getTpccTable("CUSTOMER");

  expr_list_t expressions;
  expressions.push_back(new GenericExpressionValue<hyrise_int_t, std::equal_to<hyrise_int_t>>(customer->getDeltaTable(), "C_W_ID", _w_id));
  expressions.push_back(new GenericExpressionValue<hyrise_int_t, std::equal_to<hyrise_int_t>>(customer->getDeltaTable(), "C_D_ID", _d_id));
  expressions.push_back(new GenericExpressionValue<hyrise_int_t, std::equal_to<hyrise_int_t>>(customer->getDeltaTable(), "C_ID", _c_id));
  auto validated = selectAndValidate(customer, "CUSTOMER", connectAnd(expressions));

  return validated;
}

storage::c_atable_ptr_t TpccOrderStatusProcedure::getCustomersByLastName() {
  auto customer = getTpccTable("CUSTOMER");

  expr_list_t expressions;
  expressions.push_back(new GenericExpressionValue<hyrise_int_t, std::equal_to<hyrise_int_t>>(customer->getDeltaTable(), "C_W_ID", _w_id));
  expressions.push_back(new GenericExpressionValue<hyrise_int_t, std::equal_to<hyrise_int_t>>(customer->getDeltaTable(), "C_D_ID", _d_id));
  expressions.push_back(new GenericExpressionValue<hyrise_string_t, std::equal_to<hyrise_string_t>>(customer->getDeltaTable(), "C_LAST", _c_last));
  auto validated = selectAndValidate(customer, "CUSTOMER", connectAnd(expressions));

  auto sorted = sort(validated, "C_FIRST", true);
  return sorted;
}

storage::c_atable_ptr_t TpccOrderStatusProcedure::getLastOrder() {
  auto orders = getTpccTable("ORDERS");

  expr_list_t expressions;
  expressions.push_back(new GenericExpressionValue<hyrise_int_t, std::equal_to<hyrise_int_t>>(orders->getDeltaTable(), "O_W_ID", _w_id));
  expressions.push_back(new GenericExpressionValue<hyrise_int_t, std::equal_to<hyrise_int_t>>(orders->getDeltaTable(), "O_D_ID", _d_id));
  expressions.push_back(new GenericExpressionValue<hyrise_int_t, std::equal_to<hyrise_int_t>>(orders->getDeltaTable(), "O_C_ID", _c_id));
  auto validated = selectAndValidate(orders, "ORDERS", connectAnd(expressions));

  auto sorted = sort(validated, "O_ID", false);
  return sorted;
}

storage::c_atable_ptr_t TpccOrderStatusProcedure::getOrderLines() {
  auto order_line = getTpccTable("ORDER_LINE");

  expr_list_t expressions;
  expressions.push_back(new GenericExpressionValue<hyrise_int_t, std::equal_to<hyrise_int_t>>(order_line->getDeltaTable(), "OL_W_ID", _w_id));
  expressions.push_back(new GenericExpressionValue<hyrise_int_t, std::equal_to<hyrise_int_t>>(order_line->getDeltaTable(), "OL_D_ID", _d_id));
  expressions.push_back(new GenericExpressionValue<hyrise_int_t, std::equal_to<hyrise_int_t>>(order_line->getDeltaTable(), "OL_O_ID", _o_id));
  auto validated = selectAndValidate(order_line, "ORDER_LINE", connectAnd(expressions));

  return validated;
}

} } // namespace hyrise::access

