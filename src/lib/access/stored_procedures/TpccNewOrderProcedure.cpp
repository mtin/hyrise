// Copyright (c) 2013 Hasso-Plattner-Institut fuer Softwaresystemtechnik GmbH. All rights reserved.

#include "TpccNewOrderProcedure.h"

#include <iomanip>

#include <helper/make_unique.h>
#include <storage/AbstractTable.h>
#include <access.h>

namespace hyrise { namespace access {

namespace {
  auto _ = net::Router::registerRoute<TpccNewOrderProcedure>("/TPCC-NewOrder/");
} // namespace


TpccNewOrderProcedure::TpccNewOrderProcedure(net::AbstractConnection* connection) :
  TpccStoredProcedure(connection) {
}

void TpccNewOrderProcedure::setData(const Json::Value& data) {
  _w_id = assureMemberExists(data, "W_ID").asInt();
  _d_id = assureIntValueBetween(data, "D_ID", 1, 10);
  _c_id = assureMemberExists(data, "C_ID").asInt();

  const auto itemInfo = assureMemberExists(data, "items");
  _ol_cnt = static_cast<size_t>(itemInfo.size());
  if (_ol_cnt < 5 || _ol_cnt > 15)
    throw std::runtime_error("there must be between 5 and 15 items in an order");

  for (Json::ArrayIndex i = 0; i < static_cast<Json::ArrayIndex>(_ol_cnt); ++i) {
    ItemInfo info;
    info.id       = assureMemberExists(itemInfo[i],"I_ID").asInt();
    info.w_id     = assureMemberExists(itemInfo[i],"I_W_ID").asInt();
    info.quantity = assureIntValueBetween(itemInfo[i],"quantity", 1, 10);
    _items.push_back(info);
  }
  _carrier_id = 0;
}

std::string TpccNewOrderProcedure::name() {
  return "TPCC-NewOrder";
}

const std::string TpccNewOrderProcedure::vname() {
  return "TPCC-NewOrder";
}

int TpccNewOrderProcedure::allLocal() const {
  if (std::all_of(_items.cbegin(), _items.cend(), [&] (const ItemInfo& item) { return item.w_id == _w_id; }))
    return 1;
  return 0;
}

Json::Value TpccNewOrderProcedure::execute() {
  _date = getDate();
  _all_local = allLocal();

  _rollback = false;
  for (size_t i = 0; i < _ol_cnt; ++i) {
    auto& item = _items.at(i);
    auto tItem = getItemInfo(item.id);
    if (tItem->size() == 0) {
      --_ol_cnt;
      _items.erase(_items.begin() + i);
      _rollback = true;
      if (i != _ol_cnt)
        throw std::runtime_error("only the last item may be invalid!");
      continue; // do NOT add this item to the itemlist
    }
    item.price = tItem->getValue<hyrise_float_t>("I_PRICE", 0);
    item.name = tItem->getValue<hyrise_string_t>("I_NAME", 0);
    const auto i_data = tItem->getValue<hyrise_string_t>("I_DATA", 0);
    item.original = (i_data.find("ORIGINAL") != i_data.npos);
  }

  auto tWarehouse = getWarehouseTaxRate();
  if (tWarehouse->size() == 0) {
    std::ostringstream os;
    os << "no warehouse with id: " << _w_id;
    throw std::runtime_error(os.str());
  }
  const float w_tax = tWarehouse->getValue<hyrise_float_t>("W_TAX", 0);

  bool holds_district_lock = false;
  try {
    std::const_pointer_cast<storage::Store>(getTpccTable("DISTRICT"))->lock();
    holds_district_lock = true;

    auto tDistrict = getDistrict();
    if (tDistrict->size() == 0) {
      std::ostringstream os;
      os << "no district with id " << _d_id << " for warehouse " << _w_id;
      throw std::runtime_error(os.str());
    }
    const float d_tax = tDistrict->getValue<hyrise_float_t>("D_TAX", 0);
    _o_id = tDistrict->getValue<hyrise_int_t>("D_NEXT_O_ID", 0);

    auto tCustomer = getCustomer();
    if (tCustomer->size() == 0) {
      std::ostringstream os;
      os << "no customer with id: " << _c_id;
      throw std::runtime_error(os.str());
    }


    const float c_discount = tCustomer->getValue<hyrise_float_t>("C_DISCOUNT", 0);
    const std::string c_last = tCustomer->getValue<hyrise_string_t>("C_LAST", 0);
    const std::string c_credit = tCustomer->getValue<hyrise_string_t>("C_CREDIT", 0);

    incrementNextOrderId(std::const_pointer_cast<storage::AbstractTable>(tDistrict));
    createOrder();
    createNewOrder();

    int s_ytd;
    std::string s_data, s_dist;
    float total = 0;

    for (Json::ArrayIndex i = 0; i < static_cast<Json::ArrayIndex>(_ol_cnt); ++i) {
      auto& item = _items.at(i);
      const int quantity = _items.at(i).quantity;

      auto tStock = getStockInfo(item);
      if (tStock->size() == 0) {
        std::ostringstream os;
        os << "internal error: no stock information for item: " << item.id << " in warehouse " << item.w_id;
        throw std::runtime_error(os.str());
      }

      s_ytd = tStock->getValue<hyrise_int_t>("S_YTD", 0);
      s_ytd += quantity;

      int s_quantity = tStock->getValue<hyrise_int_t>("S_QUANTITY", 0);
      item.s_order_cnt = tStock->getValue<hyrise_int_t>("S_ORDER_CNT", 0);

      if (s_quantity >= quantity + 10) {
        s_quantity = s_quantity - quantity;
      }
      else {
        s_quantity = s_quantity + 91 - quantity;
        ++item.s_order_cnt;
      }
      item.s_quantity = s_quantity;
      item.s_remote_cnt = tStock->getValue<hyrise_int_t>("S_REMOTE_CNT", 0);

      const auto s_data = tStock->getValue<hyrise_string_t>("S_DATA", 0);
      item.original &= s_data.find("ORIGINAL") != s_data.npos;

      std::ostringstream os;
      os << "S_DIST_" << std::setw(2) << std::setfill('0') << std::right << _d_id;
      const std::string s_dist_name = os.str(); //e.g. S_DIST_01
      _ol_dist_info = tStock->getValue<hyrise_string_t>(s_dist_name, 0);

      total += item.amount();

      updateStock(std::const_pointer_cast<storage::AbstractTable>(tStock), item);
      createOrderLine(item, i + 1);
    }

    if (_rollback) {
      rollback();
    } else {
      commit();
    }

    std::const_pointer_cast<storage::Store>(getTpccTable("DISTRICT"))->unlock();
    holds_district_lock = false;

    Json::Value result;
    result["W_ID"] = _w_id;
    result["D_ID"] = _d_id;
    result["C_ID"] = _c_id;
    result["C_LAST"] = c_last;
    result["C_CREDIT"] = c_credit;
    result["C_DISCOUNT"] = c_discount;
    result["W_TAX"] = w_tax;
    result["D_TAX"] = d_tax;
    result["O_OL_CNT"] = _ol_cnt;
    result["O_ID"] = _o_id;
    result["D_NEXT_O_ID"] = _o_id+1;
    result["O_ENTRY_D"] = _date;

    if (_rollback) {
      result["total-amount"] = "Item number is not valid";
      return result;
    }
    //needed for what?
    total *=  (1 - c_discount) * (1 + w_tax + d_tax);
    result["total-amount"] = total;

    Json::Value items(Json::arrayValue);
    for (Json::ArrayIndex i = 0; i < static_cast<Json::ArrayIndex>(_ol_cnt); ++i) {
      Json::Value item;
      const auto& cur = _items.at(i);
      item["OL_SUPPLY_W_ID"] = cur.w_id;
      item["OL_I_ID"] = cur.id;
      item["I_NAME"] = cur.name;
      item["OL_QUANTITY"] = cur.quantity;
      item["S_QUANTITY"] = cur.s_quantity;
      if (cur.original)
        item["brand-generic"] = "B";
      else
        item["brand-generic"] = "G";
      item["I_PRICE"] = cur.price;
      item["OL_AMOUNT"] = cur.price * cur.quantity;

      items.append(item);
    }
    result["items"] = items;

    return result;
  } catch(std::runtime_error &e) {
    if(holds_district_lock) std::const_pointer_cast<storage::Store>(getTpccTable("DISTRICT"))->unlock();
    throw;
    // TODO: Switch to scoped spinlock
  }

}

void TpccNewOrderProcedure::createNewOrder() {
  auto newOrder = std::const_pointer_cast<storage::Store>(getTpccTable("NEW_ORDER"));
  auto newRow = newRowFrom(newOrder);
  newRow->setValue<hyrise_int_t>("NO_O_ID", 0, _o_id);
  newRow->setValue<hyrise_int_t>("NO_D_ID", 0, _d_id);
  newRow->setValue<hyrise_int_t>("NO_W_ID", 0, _w_id);

  insert(newOrder, newRow);
}

void TpccNewOrderProcedure::createOrderLine(const ItemInfo& item, const int ol_number) {
  auto orderLine = std::const_pointer_cast<storage::Store>(getTpccTable("ORDER_LINE"));
  auto newRow = newRowFrom(orderLine);
  newRow->setValue<hyrise_int_t>("OL_O_ID", 0, _o_id);
  newRow->setValue<hyrise_int_t>("OL_D_ID", 0, _d_id);
  newRow->setValue<hyrise_int_t>("OL_W_ID", 0, _w_id);
  newRow->setValue<hyrise_int_t>("OL_NUMBER", 0, ol_number);
  newRow->setValue<hyrise_int_t>("OL_I_ID", 0, item.id);
  newRow->setValue<hyrise_int_t>("OL_SUPPLY_W_ID", 0, item.w_id);
  newRow->setValue<hyrise_string_t>("OL_DELIVERY_D", 0, _date);
  newRow->setValue<hyrise_int_t>("OL_QUANTITY", 0, item.quantity);
  newRow->setValue<hyrise_float_t>("OL_AMOUNT", 0, item.amount());
  newRow->setValue<hyrise_string_t>("OL_DIST_INFO", 0, _ol_dist_info);

  insert(orderLine, newRow);
}

void TpccNewOrderProcedure::createOrder() {
  auto orders = std::const_pointer_cast<storage::Store>(getTpccTable("ORDERS"));
  auto newRow = newRowFrom(orders);
  newRow->setValue<hyrise_int_t>("O_ID", 0, _o_id);
  newRow->setValue<hyrise_int_t>("O_D_ID", 0, _d_id);
  newRow->setValue<hyrise_int_t>("O_W_ID", 0, _w_id);
  newRow->setValue<hyrise_int_t>("O_C_ID", 0, _c_id);
  newRow->setValue<hyrise_string_t>("O_ENTRY_D", 0, _date);
  newRow->setValue<hyrise_int_t>("O_CARRIER_ID", 0, _carrier_id);
  newRow->setValue<hyrise_int_t>("O_OL_CNT", 0, _ol_cnt);
  newRow->setValue<hyrise_int_t>("O_ALL_LOCAL", 0, _all_local);

  insert(orders, newRow);
}

storage::c_atable_ptr_t TpccNewOrderProcedure::getCustomer() {
  auto customer = getTpccTable("CUSTOMER");

  expr_list_t expressions;
  expressions.push_back(new GenericExpressionValue<hyrise_int_t, std::equal_to<hyrise_int_t>>(customer->getDeltaTable(), "C_W_ID", _w_id));
  expressions.push_back(new GenericExpressionValue<hyrise_int_t, std::equal_to<hyrise_int_t>>(customer->getDeltaTable(), "C_D_ID", _d_id));
  expressions.push_back(new GenericExpressionValue<hyrise_int_t, std::equal_to<hyrise_int_t>>(customer->getDeltaTable(), "C_ID", _c_id));
  auto validated = selectAndValidate(customer, "CUSTOMER", connectAnd(expressions));

  return validated;
}

storage::c_atable_ptr_t TpccNewOrderProcedure::getDistrict() {
  auto district = getTpccTable("DISTRICT");

  expr_list_t expressions;
  expressions.push_back(new GenericExpressionValue<hyrise_int_t, std::equal_to<hyrise_int_t>>(district->getDeltaTable(), "D_W_ID", _w_id));
  expressions.push_back(new GenericExpressionValue<hyrise_int_t, std::equal_to<hyrise_int_t>>(district->getDeltaTable(), "D_ID", _d_id));
  auto validated = selectAndValidate(district, "DISTRICT", connectAnd(expressions));

  return validated;
}

storage::c_atable_ptr_t TpccNewOrderProcedure::getItemInfo(int i_id) {
  auto item = getTpccTable("ITEM");

  auto validated = selectAndValidate(item, "ITEM", std::unique_ptr<SimpleExpression>(new GenericExpressionValue<hyrise_int_t, std::equal_to<hyrise_int_t>>(item->getDeltaTable(), "I_ID", i_id)));

  return validated;
}

storage::c_atable_ptr_t TpccNewOrderProcedure::getStockInfo(const ItemInfo& item) {
  auto stock = getTpccTable("STOCK");

  expr_list_t expressions;
  expressions.push_back(new GenericExpressionValue<hyrise_int_t, std::equal_to<hyrise_int_t>>(stock->getDeltaTable(), "S_W_ID", item.w_id));
  expressions.push_back(new GenericExpressionValue<hyrise_int_t, std::equal_to<hyrise_int_t>>(stock->getDeltaTable(), "S_I_ID", item.id));
  auto validated = selectAndValidate(stock, "STOCK", connectAnd(expressions));

  return validated;
}

storage::c_atable_ptr_t TpccNewOrderProcedure::getWarehouseTaxRate() {
  auto warehouse = getTpccTable("WAREHOUSE");

  auto validated = selectAndValidate(warehouse, "WAREHOUSE", std::unique_ptr<SimpleExpression>(new GenericExpressionValue<hyrise_int_t, std::equal_to<hyrise_int_t>>(warehouse->getDeltaTable(), "W_ID", _w_id)));

  return validated;
}

void TpccNewOrderProcedure::incrementNextOrderId(const storage::atable_ptr_t& districtRow) {
  Json::Value updates;
  updates["D_NEXT_O_ID"] = _o_id + 1;
  update(districtRow, updates);
}

void TpccNewOrderProcedure::updateStock(const storage::atable_ptr_t& stockRow, const ItemInfo& item) {
  Json::Value updates;
  updates["S_QUANTITY"] = item.quantity;
  updates["S_ORDER_CNT"] = item.s_order_cnt;
  updates["S_REMOTE_CNT"] = item.s_remote_cnt;
  updates["S_QUANTITY"] = item.quantity;
  update(stockRow, updates);
}

} } // namespace hyrise::access

