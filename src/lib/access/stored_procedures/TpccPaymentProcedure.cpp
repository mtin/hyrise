// Copyright (c) 2013 Hasso-Plattner-Institut fuer Softwaresystemtechnik GmbH. All rights reserved.

#include "TpccPaymentProcedure.h"

#include <storage/AbstractTable.h>
#include <storage/TableBuilder.h>
#include <storage/storage_types_helper.h>
#include <access.h>

namespace hyrise { namespace access {

namespace {
  auto _ = net::Router::registerRoute<TpccPaymentProcedure>("/TPCC-Payment/");
} // namespace


TpccPaymentProcedure::TpccPaymentProcedure(net::AbstractConnection* connection) :
  TpccStoredProcedure(connection) {
}

void TpccPaymentProcedure::setData(const Json::Value& data) {
  _w_id =     assureMemberExists(data, "W_ID").asInt();
  _d_id =     assureIntValueBetween(data, "D_ID", 1, 10);
  _c_w_id =   assureMemberExists(data, "C_W_ID").asInt();
  _c_d_id =   assureIntValueBetween(data, "C_D_ID", 1, 10);
  _h_amount = assureFloatValueBetween(data, "H_AMOUNT", 1.0f, 5000.0f);

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

std::string TpccPaymentProcedure::name() {
  return "TPCC-Payment";
}

const std::string TpccPaymentProcedure::vname() {
  return "TPCC-Payment";
}

Json::Value TpccPaymentProcedure::execute() {
  _date = getDate();

  std::shared_ptr<const AbstractTable> tCustomer;
  if (_customerById) {
    tCustomer = std::const_pointer_cast<const AbstractTable>(getCustomerByCId());
    if (tCustomer->size() == 0) {
      std::ostringstream os;
      os << "no customer with id " << _c_id << " in district " << _d_id << " in warehouse " << _w_id;
      throw std::runtime_error(os.str());
    }
    _chosenOne = 0;
    _c_last = tCustomer->getValue<hyrise_string_t>("C_LAST", 0);
  }
  else {
    tCustomer = std::const_pointer_cast<const AbstractTable>(getCustomersByLastName());
    if (tCustomer->size() == 0) {
      std::ostringstream os;
      os << "no customer with last name \"" << _c_last << "\" in district " << _d_id << " in warehouse " << _w_id;
      throw std::runtime_error(os.str());
    }
    _chosenOne = std::max<int>((tCustomer->size() - 1) / 2 - 1, 0); //really -1 again?;
    _c_id = tCustomer->getValue<hyrise_int_t>("C_ID", _chosenOne);
  }

  const std::string c_credit = tCustomer->getValue<hyrise_string_t>("C_CREDIT", _chosenOne);
  const bool bc_customer = (c_credit == "BC");
  _c_payment_cnt = tCustomer->getValue<hyrise_int_t>("C_PAYMENT_CNT", _chosenOne) + 1;
  _c_balance = tCustomer->getValue<hyrise_float_t>("C_BALANCE", _chosenOne) + _h_amount;
  _c_ytd_payment = tCustomer->getValue<hyrise_float_t>("C_YTD_PAYMENT", _chosenOne) + _h_amount;
  _c_data = tCustomer->getValue<hyrise_string_t>("C_DATA", _chosenOne);

  bool holds_warehouse_lock = false, holds_district_lock = false;

  try {
    std::const_pointer_cast<storage::Store>(getTpccTable("WAREHOUSE"))->lock();
    holds_warehouse_lock = true;
    auto tWarehouse = getWarehouse();
    if (tWarehouse->size() == 0) {
      std::ostringstream os;
      os << "no such warehouse: " << _w_id;
      throw std::runtime_error(os.str());
    }
    _w_ytd = tWarehouse->getValue<hyrise_float_t>("W_YTD", 0) + _h_amount;
    const std::string w_name = tWarehouse->getValue<hyrise_string_t>("W_NAME", 0);

    std::const_pointer_cast<storage::Store>(getTpccTable("DISTRICT"))->lock();
    holds_district_lock = true;
    auto tDistrict = getDistrict();
    if (tDistrict->size() == 0) {
      std::ostringstream os;
      os << "internal error: no district " << _d_id << " for warehouse " << _w_id;
      throw std::runtime_error(os.str());
    }
    _d_ytd = tDistrict->getValue<hyrise_float_t>("D_YTD", 0) + _h_amount;
    const std::string d_name = tDistrict->getValue<hyrise_string_t>("D_NAME", 0);
    _h_data = w_name + "    " + d_name;

    updateWarehouseBalance(std::const_pointer_cast<AbstractTable>(tWarehouse));
    updateDistrictBalance(std::const_pointer_cast<AbstractTable>(tDistrict));

    if (bc_customer)
      updateBCCustomer(std::const_pointer_cast<AbstractTable>(tCustomer));
    else
      updateGCCustomer(std::const_pointer_cast<AbstractTable>(tCustomer));

    insertHistory();

    commit();

    std::const_pointer_cast<storage::Store>(getTpccTable("DISTRICT"))->unlock();
    holds_district_lock = false;
    std::const_pointer_cast<storage::Store>(getTpccTable("WAREHOUSE"))->unlock();
    holds_warehouse_lock = false;

    Json::Value result;
    result["W_ID"]          = _w_id;
    result["D_ID"]          = _d_id;
    result["C_ID"]          = _c_id;
    result["C_W_ID"]        = _c_w_id;
    result["C_D_ID"]        = _c_d_id;
    result["H_AMOUNT"]      = _h_amount;
    result["H_DATE"]        = _date;

    result["W_STREET_1"]    = tWarehouse->getValue<hyrise_string_t>("W_STREET_1", 0);
    result["W_STREET_2"]    = tWarehouse->getValue<hyrise_string_t>("W_STREET_2", 0);
    result["W_CITY"]        = tWarehouse->getValue<hyrise_string_t>("W_CITY", 0);
    result["W_STATE"]       = tWarehouse->getValue<hyrise_string_t>("W_STATE", 0);
    result["W_ZIP"]         = tWarehouse->getValue<hyrise_string_t>("W_ZIP", 0);

    result["D_STREET_1"]    = tDistrict->getValue<hyrise_string_t>("D_STREET_1", 0);
    result["D_STREET_2"]    = tDistrict->getValue<hyrise_string_t>("D_STREET_2", 0);
    result["D_CITY"]        = tDistrict->getValue<hyrise_string_t>("D_CITY", 0);
    result["D_STATE"]       = tDistrict->getValue<hyrise_string_t>("D_STATE", 0);
    result["D_ZIP"]         = tDistrict->getValue<hyrise_string_t>("D_ZIP", 0);

    result["C_FIRST"]       = tCustomer->getValue<hyrise_string_t>("C_FIRST", _chosenOne);
    result["C_MIDDLE"]      = tCustomer->getValue<hyrise_string_t>("C_MIDDLE", _chosenOne);
    result["C_LAST"]        = _c_last;
    result["C_STREET_1"]    = tCustomer->getValue<hyrise_string_t>("C_STREET_1", _chosenOne);
    result["C_STREET_2"]    = tCustomer->getValue<hyrise_string_t>("C_STREET_2", _chosenOne);
    result["C_CITY"]        = tCustomer->getValue<hyrise_string_t>("C_CITY", _chosenOne);
    result["C_STATE"]       = tCustomer->getValue<hyrise_string_t>("C_STATE", _chosenOne);
    result["C_ZIP"]         = tCustomer->getValue<hyrise_string_t>("C_ZIP", _chosenOne);
    result["C_PHONE"]       = tCustomer->getValue<hyrise_string_t>("C_PHONE", _chosenOne);
    result["C_SINCE"]       = tCustomer->getValue<hyrise_string_t>("C_SINCE", _chosenOne);
    result["C_CREDIT"]      = c_credit;
    result["C_CREDIT_LIM"]  = tCustomer->getValue<hyrise_float_t>("C_CREDIT_LIM", _chosenOne);
    result["C_DISCOUNT"]    = tCustomer->getValue<hyrise_float_t>("C_DISCOUNT", _chosenOne);
    result["C_BALANCE"]     = _c_balance;

    //only if BC? don't know
    result["C_DATA"]        = _c_data;
    return result;
  } catch(std::runtime_error &e) {
    if(holds_district_lock) std::const_pointer_cast<storage::Store>(getTpccTable("DISTRICT"))->unlock();
    if(holds_warehouse_lock) std::const_pointer_cast<storage::Store>(getTpccTable("WAREHOUSE"))->unlock();
    throw;
    // TODO: Switch to scoped spinlock
  }
}

storage::c_atable_ptr_t TpccPaymentProcedure::getCustomerByCId() {
  auto customer = getTpccTable("CUSTOMER");

  expr_list_t expressions;
  expressions.push_back(new GenericExpressionValue<hyrise_int_t, std::equal_to<hyrise_int_t>>(customer->getDeltaTable(), "C_W_ID", _w_id));
  expressions.push_back(new GenericExpressionValue<hyrise_int_t, std::equal_to<hyrise_int_t>>(customer->getDeltaTable(), "C_D_ID", _d_id));
  expressions.push_back(new GenericExpressionValue<hyrise_int_t, std::equal_to<hyrise_int_t>>(customer->getDeltaTable(), "C_ID", _c_id));
  auto validated = selectAndValidate(customer, "CUSTOMER", connectAnd(expressions));

  return validated;
}

storage::c_atable_ptr_t TpccPaymentProcedure::getCustomersByLastName() {
  auto customer = getTpccTable("CUSTOMER");

  expr_list_t expressions;
  expressions.push_back(new GenericExpressionValue<hyrise_int_t, std::equal_to<hyrise_int_t>>(customer->getDeltaTable(), "C_W_ID", _w_id));
  expressions.push_back(new GenericExpressionValue<hyrise_int_t, std::equal_to<hyrise_int_t>>(customer->getDeltaTable(), "C_D_ID", _d_id));
  expressions.push_back(new GenericExpressionValue<hyrise_string_t, std::equal_to<hyrise_string_t>>(customer->getDeltaTable(), "C_LAST", _c_last));
  auto validated = selectAndValidate(customer, "CUSTOMER", connectAnd(expressions));

  auto sorted = sort(validated, "C_FIRST", true);
  return sorted;
}

storage::c_atable_ptr_t TpccPaymentProcedure::getDistrict() {
  auto district = getTpccTable("DISTRICT");

  expr_list_t expressions;
  expressions.push_back(new GenericExpressionValue<hyrise_int_t, std::equal_to<hyrise_int_t>>(district->getDeltaTable(), "D_W_ID", _w_id));
  expressions.push_back(new GenericExpressionValue<hyrise_int_t, std::equal_to<hyrise_int_t>>(district->getDeltaTable(), "D_ID", _d_id));
  auto validated = selectAndValidate(district, "DISTRICT", connectAnd(expressions));

  return validated;
}

storage::c_atable_ptr_t TpccPaymentProcedure::getWarehouse() {
  auto warehouse = getTpccTable("WAREHOUSE");

  auto validated = selectAndValidate(warehouse, "WAREHOUSE", std::unique_ptr<SimpleExpression>(new GenericExpressionValue<hyrise_int_t, std::equal_to<hyrise_int_t>>(warehouse->getDeltaTable(), "W_ID", _w_id)));

  return validated;
}

void TpccPaymentProcedure::insertHistory() {
  auto history = std::const_pointer_cast<storage::Store>(getTpccTable("HISTORY"));

  auto newRow = newRowFrom(history);
  newRow->setValue<hyrise_int_t>("H_C_ID", 0, _c_id);
  newRow->setValue<hyrise_int_t>("H_C_D_ID", 0, _c_d_id);
  newRow->setValue<hyrise_int_t>("H_C_W_ID", 0, _c_w_id);
  newRow->setValue<hyrise_int_t>("H_D_ID", 0, _d_id);
  newRow->setValue<hyrise_int_t>("H_W_ID", 0, _w_id);
  newRow->setValue<hyrise_string_t>("H_DATE", 0, _date);
  newRow->setValue<hyrise_float_t>("H_AMOUNT", 0, _h_amount);
  newRow->setValue<hyrise_string_t>("H_DATA", 0, _h_data);

  insert(history, newRow);
}

void TpccPaymentProcedure::updateDistrictBalance(const storage::atable_ptr_t& districtRow) {
  Json::Value updates;
  updates["D_YTD"] = _d_ytd;
  update(districtRow, updates);
}

void TpccPaymentProcedure::updateBCCustomer(const storage::atable_ptr_t& customerRow) {
  Json::Value updates;
  updates["C_BALANCE"] = _c_balance;
  updates["C_YTD_PAYMENT"] = _c_ytd_payment;
  updates["C_PAYMENT_CNT"] = _c_payment_cnt;
  updates["C_DATA"] = _c_data;
  update(customerRow, updates);
}

void TpccPaymentProcedure::updateGCCustomer(const storage::atable_ptr_t& customerRow) {
  Json::Value updates;
  updates["C_BALANCE"] = _c_balance;
  updates["C_YTD_PAYMENT"] = _c_ytd_payment;
  updates["C_PAYMENT_CNT"] = _c_payment_cnt;
  update(customerRow, updates);
}

void TpccPaymentProcedure::updateWarehouseBalance(const storage::atable_ptr_t& warehouseRow) {
  Json::Value updates;
  updates["W_YTD"] = _w_ytd;
  update(warehouseRow, updates);
}

} } // namespace hyrise::access

