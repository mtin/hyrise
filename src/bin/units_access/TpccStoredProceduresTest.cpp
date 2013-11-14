// Copyright (c) 2012 Hasso-Plattner-Institut fuer Softwaresystemtechnik GmbH. All rights reserved.
#include "io/shortcuts.h"
#include "testing/test.h"

#include "helper.h"
#include <fstream>
#include <json.h>

#include <io/ResourceManager.h>

namespace hyrise {
namespace access {

namespace {
  typedef struct item_t { int id; int wid; int quantity; } ItemInfo;
  typedef std::vector<ItemInfo> item_list_t;
} // namespace

class TpccError : public std::runtime_error {
 public:
  TpccError(const std::string& what) : std::runtime_error(what) {}
};

class TpccStoredProceduresTest : public AccessTest {
public:
  void SetUp() {
    loadTables();

    i_customer_size  = getTable(Customer)->size();
    i_orders_size    = getTable(Orders)->size();
    i_orderLine_size = getTable(OrderLine)->size();
    i_warehouse_size = getTable(Warehouse)->size();
    i_newOrder_size  = getTable(NewOrder)->size();
    i_district_size  = getTable(District)->size();
    i_item_size      = getTable(Item)->size();
    i_stock_size     = getTable(Stock)->size();
    i_history_size   = getTable(History)->size();
  }

  void TearDown() {
    io::ResourceManager::getInstance().clear();
  }

protected:
  void loadTables() {
    executeAndWait(loadFromFile("test/tpcc/load_tpcc_tables.json"));
  }

  Json::Value doDelivery(int w_id, int d_id, int o_carrier_id);
  Json::Value doNewOrder(int w_id, int d_id, int c_id, int o_carrier_id, const std::string& ol_dist_info, item_list_t items);
  Json::Value doNewOrderR(int w_id, int d_id, int c_id, int o_carrier_id, const std::string& ol_dist_info, item_list_t items);
  Json::Value doOrderStatus(int w_id, int d_id, int c_id, const std::string& c_last);
  Json::Value doPayment(int w_id, int d_id, int c_id, const std::string& c_last, int c_w_id, int c_d_id, float h_amount, bool bc_customer);
  Json::Value doStockLevel(int w_id, int d_id, int threshold);

  size_t i_customer_size, i_orders_size, i_orderLine_size, i_warehouse_size, i_newOrder_size,
         i_district_size, i_item_size, i_stock_size, i_history_size;


  enum TpccTable { Customer, Orders, OrderLine, Warehouse, NewOrder, District, Item, Stock, History };

  static storage::c_atable_ptr_t getTable(const TpccTable table, tx::transaction_id_t tid = hyrise::tx::UNKNOWN) {
    std::string tableName;
    std::string fileName;
    switch (table) {
      case Customer:  tableName = "CUSTOMER"; break;
      case Orders:    tableName = "ORDERS"; break;
      case OrderLine: tableName = "ORDER_LINE"; break;
      case Warehouse: tableName = "WAREHOUSE"; break;
      case NewOrder:  tableName = "NEW_ORDER"; break;
      case District:  tableName = "DISTRICT"; break;
      case Item:      tableName = "ITEM"; break;
      case Stock:     tableName = "STOCK"; break;
      case History:   tableName = "HISTORY"; break;
    }
    if (tid == hyrise::tx::UNKNOWN)
      tid = getNewTXContext().tid;

    return executeAndWait("{\"operators\": {\"load\": {\"type\": \"GetTable\", \"name\": \"" + tableName + "\"}" +
                          ", \"validate\": {\"type\": \"ValidatePositions\"}}, \"edges\": [[\"load\", \"validate\"]]}", 1, nullptr, tid);
  }
};

TEST_F(TpccStoredProceduresTest, LoadTables) {
  auto customer = getTable(Customer);
  EXPECT_LT(0, customer->size()) << "invalid customer table";

  auto orders = getTable(Orders);
  EXPECT_LT(0, orders->size()) << "invalid orders table";
  
  auto orderLine = getTable(OrderLine);
  EXPECT_LT(0, orderLine->size()) << "invalid order_line table";
  
  auto warehouse = getTable(Warehouse);
  EXPECT_LT(0, warehouse->size()) << "invalid warehouse table";
  
  auto newOrder = getTable(NewOrder);
  EXPECT_LT(0, newOrder->size()) << "invalid new_order table";
  
  auto district = getTable(District);
  EXPECT_LT(0, district->size()) << "invalid district table";
  
  auto item = getTable(Item);
  EXPECT_LT(0, item->size()) << "invalid item table";
  
  auto stock = getTable(Stock);
  EXPECT_LT(0, stock->size()) << "invalid stock table";
  
  auto history = getTable(History);
  EXPECT_LT(0, history->size()) << "invalid history table";
}

//TEST_F(TpccStoredProceduresTest, DeliveryTest) {
Json::Value TpccStoredProceduresTest::doDelivery(int w_id, int d_id, int o_carrier_id) {
  Json::Value data;
  data["W_ID"] = w_id;
  data["D_ID"] = d_id;
  data["O_CARRIER_ID"] = o_carrier_id;

  Json::StyledWriter writer;
  const auto s = executeStoredProcedureAndWait("TPCC-Delivery", writer.write(data));

  Json::Reader reader;
  Json::Value response;
  if (!reader.parse(s, response))
    throw TpccError(s);
  return response;
}

namespace {
  Json::Value newOrderData(int w_id, int d_id, int c_id, int o_carrier_id, const std::string& ol_dist_info, item_list_t items) {
    Json::Value data;
    data["W_ID"] = w_id;
    data["D_ID"] = d_id;
    data["C_ID"] = c_id;
    data["O_CARRIER_ID"] = o_carrier_id;
    data["OL_DIST_INFO"] = ol_dist_info;

    Json::Value itemData(Json::arrayValue);
    for (size_t i = 0; i < items.size(); ++i) {
       Json::Value item;
       item["I_ID"] = items.at(i).id;
       item["I_W_ID"] = items.at(i).wid;
       item["quantity"] = items.at(i).quantity;
       itemData.append(item);
    }
    data["items"] = itemData;

    return data;
  }
} // namespace

//TEST_F(TpccStoredProceduresTest, NewOrderTest) {
Json::Value TpccStoredProceduresTest::doNewOrder(int w_id, int d_id, int c_id, int o_carrier_id, const std::string& ol_dist_info,
                                                 item_list_t items) {
  auto data = newOrderData(w_id, d_id, c_id, o_carrier_id, ol_dist_info, items);

  Json::StyledWriter writer;
  const auto s = executeStoredProcedureAndWait("TPCC-NewOrder", writer.write(data));
  Json::Reader reader;
  Json::Value response;
  if (!reader.parse(s, response))
    throw TpccError(s);

  return response;
}

//TEST_F(TpccStoredProceduresTest, NewOrderTest) {
Json::Value TpccStoredProceduresTest::doNewOrderR(int w_id, int d_id, int c_id, int o_carrier_id, const std::string& ol_dist_info,
                                                  const item_list_t items) {
  auto data = newOrderData(w_id, d_id, c_id, o_carrier_id, ol_dist_info, items);

  Json::StyledWriter writer;
  const auto s = executeStoredProcedureAndWait("TPCC-NewOrder", writer.write(data));
  Json::Reader reader;
  Json::Value response;
  if (!reader.parse(s, response))
    throw TpccError(s);
  
  return response;
}

//TEST_F(TpccStoredProceduresTest, OrderStatusTest) {
Json::Value TpccStoredProceduresTest::doOrderStatus(int w_id, int d_id, int c_id, const std::string& c_last) {
  bool selectById = (c_last.size() == 0);

  Json::Value data;
  data["W_ID"] = w_id;
  data["D_ID"] = d_id;
  if (selectById)
    data["C_ID"] = d_id;
  else
    data["C_LAST"] = c_last;

  Json::StyledWriter writer;

  const auto s = executeStoredProcedureAndWait("TPCC-OrderStatus", writer.write(data));
  Json::Reader reader;
  Json::Value response;
  if (!reader.parse(s, response))
    throw TpccError(s);

  return response;
}

Json::Value TpccStoredProceduresTest::doPayment(int w_id, int d_id, int c_id, const std::string& c_last, int c_w_id,
                                                int c_d_id, float h_amount, bool bc_customer) {
  const bool selectById = (c_last.size() == 0);

  Json::Value data;
  data["W_ID"] = w_id;
  data["D_ID"] = d_id;

  if (selectById)
    data["C_ID"] = c_id;
  else
    data["C_LAST"] = c_last;

  data["C_W_ID"] = c_w_id;
  data["C_D_ID"] = c_d_id;
  data["H_AMOUNT"] = h_amount;

  Json::StyledWriter writer;
    
  const auto s = executeStoredProcedureAndWait("TPCC-Payment", writer.write(data));
  Json::Reader reader;
  Json::Value response;
  if (!reader.parse(s, response))
    throw TpccError(s);

  return response;
}

Json::Value TpccStoredProceduresTest::doStockLevel(int w_id, int d_id, int threshold) {
  Json::Value data;
  data["W_ID"] = w_id;
  data["D_ID"] = d_id;
  data["threshold"] = threshold;

  Json::StyledWriter writer;
    
  const auto s = executeStoredProcedureAndWait("TPCC-StockLevel", writer.write(data));
  Json::Reader reader;
  Json::Value response;
  if (!reader.parse(s, response))
    throw TpccError(s);

  return response;
}


//===========================Test helper

void assureFieldExists(const Json::Value& data, const std::string& name) {
  if (!data.isMember(name))
    throw std::runtime_error("\'" + name + "\' should be set but is not");
}

int getValuei(const Json::Value& data, const std::string& name) {
  assureFieldExists(data, name);
  return data[name].asInt();
}

float getValuef(const Json::Value& data, const std::string& name) {
  assureFieldExists(data, name);
  return data[name].asFloat();
}

std::string getValues(const Json::Value& data, const std::string& name) {
  assureFieldExists(data, name);
  return data[name].asString();
}

//===========================Delivery Tests

TEST_F(TpccStoredProceduresTest, Delivery) {
  //                        (w_id, d_id, o_carrier_id);
  auto response = doDelivery(1   , 1   , 1           );
  EXPECT_EQ(1, getValuei(response, "W_ID"));
  EXPECT_EQ(1, getValuei(response, "O_CARRIER_ID"));

  //                   (w_id, d_id, o_carrier_id);
  response = doDelivery(1   , 2   , 10          );
  EXPECT_EQ(1, getValuei(response, "W_ID"));
  EXPECT_EQ(10, getValuei(response, "O_CARRIER_ID"));

  //                   (w_id, d_id, o_carrier_id);
  response = doDelivery(2   , 2   , 9           );
  EXPECT_EQ(2, getValuei(response, "W_ID"));
  EXPECT_EQ(9, getValuei(response, "O_CARRIER_ID"));


  //                   (w_id, d_id, o_carrier_id);
  response = doDelivery(2   , 10  , 3           );
  EXPECT_EQ(2, getValuei(response, "W_ID"));
  EXPECT_EQ(3, getValuei(response, "O_CARRIER_ID"));

}

TEST_F(TpccStoredProceduresTest, Delivery_WrongCARRIER_ID) {
  //                     (w_id, d_id, o_carrier_id);
  EXPECT_THROW(doDelivery(1   , 1   , 0           ), TpccError);
  EXPECT_THROW(doDelivery(1   , 1   , 11          ), TpccError);
}

TEST_F(TpccStoredProceduresTest, Delivery_WrongD_ID) {
  //                     (w_id, d_id, o_carrier_id);
  EXPECT_THROW(doDelivery(1   , 0   , 1           ), TpccError);
  EXPECT_THROW(doDelivery(1   , 11  , 1           ), TpccError);
}

//===========================New Order Tests

#define T_NewOrder(w_id, d_id, c_id, o_carrier_id, ol_dist_info, itemlist, o_id) \
{\
  const auto response = doNewOrder(w_id, d_id, c_id, o_carrier_id, ol_dist_info, itemlist);\
\
  EXPECT_EQ(w_id, getValuei(response, "W_ID"));\
  EXPECT_EQ(d_id, getValuei(response, "D_ID"));\
  EXPECT_EQ(c_id, getValuei(response, "C_ID"));\
  std::ostringstream os_clast;\
  os_clast << "CLName" << c_id;\
  EXPECT_EQ(os_clast.str(), getValues(response, "C_LAST"));\
  /*TODO check C_CREDIT*/\
  EXPECT_FLOAT_EQ(0.1 * w_id + 0.01 * c_id, getValuef(response, "C_DISCOUNT"));\
  EXPECT_FLOAT_EQ(0.1 * w_id, getValuef(response, "W_TAX"));\
  EXPECT_FLOAT_EQ(0.01 * d_id, getValuef(response, "D_TAX"));\
  EXPECT_EQ(itemlist.size(), getValuei(response, "O_OL_CNT"));\
  EXPECT_EQ(o_id, getValuei(response, "O_ID"));\
  getValues(response, "O_ENTRY_D");/*TODO check for value*/\
  \
  const Json::Value& items = response["items"];\
  EXPECT_EQ(itemlist.size(), items.size());\
  for (size_t i = 0; i < itemlist.size(); ++i) {\
    const Json::Value& cur = items[(int)i];\
    /*TODO this expects the response list to have the same order as the input list ...*/\
    EXPECT_EQ(itemlist.at(i).wid, getValuei(cur, "OL_SUPPLY_W_ID"));\
    EXPECT_EQ(itemlist.at(i).id, getValuei(cur, "OL_I_ID"));\
    std::ostringstream os_iname;\
    os_iname << "IName" << itemlist.at(i).id;\
    EXPECT_EQ(os_iname.str(), getValues(cur, "I_NAME"));\
    EXPECT_EQ(itemlist.at(i).quantity, getValuei(cur, "OL_QUANTITY"));\
    getValuei(cur, "S_QUANTITY");/*TODO check for value*/\
    getValues(cur, "brand-generic");/*TODO check for value*/\
    EXPECT_FLOAT_EQ(1.01 * itemlist.at(i).id, getValuef(cur, "I_PRICE"));\
    getValuef(cur, "OL_AMOUNT");/*TODO check for value*/\
  }\
  EXPECT_EQ(getTable(Customer)->size() , i_customer_size);\
  EXPECT_EQ(getTable(Orders)->size()   , i_orders_size + 1);\
  i_orders_size += 1;\
  EXPECT_EQ(getTable(OrderLine)->size(), i_orderLine_size + itemlist.size());\
  i_orderLine_size += itemlist.size();\
  EXPECT_EQ(getTable(Warehouse)->size(), i_warehouse_size);\
  EXPECT_EQ(getTable(NewOrder)->size() , i_newOrder_size + 1);\
  i_newOrder_size += 1;\
  EXPECT_EQ(getTable(District)->size() , i_district_size);\
  EXPECT_EQ(getTable(Item)->size()     , i_item_size);\
  EXPECT_EQ(getTable(Stock)->size()    , i_stock_size);\
  EXPECT_EQ(getTable(History)->size()  , i_history_size);\
}

TEST_F(TpccStoredProceduresTest, NewOrder) {
//                            {{i_id, i_w_id, quantity}}
  const item_list_t items1 = {{1   , 1     , 1       },
                              {2   , 1     , 2       },
                              {3   , 1     , 3       },
                              {4   , 1     , 4       },
                              {5   , 1     , 5       }}; // 5 items all local
  const item_list_t items2 = {{1   , 1     , 10      },
                              {2   , 1     , 10      },
                              {3   , 1     , 10      },
                              {4   , 1     , 10      },
                              {5   , 1     , 10      },
                              {6   , 1     , 10      },
                              {7   , 1     , 10      },
                              {8   , 1     , 10      },
                              {9   , 1     , 10      },
                              {10  , 1     , 10      },
                              {11  , 1     , 10      },
                              {12  , 1     , 10      },
                              {13  , 1     , 10      },
                              {14  , 1     , 10      },
                              {15  , 1     , 10      }}; // 15 items remote warehouses*/
//          (w_id, d_id, c_id, o_carrier_id, ol_dist_info, itemlist, o_id)
  T_NewOrder(1   , 1   , 1   , 1           , "info1"     , items1  , 6   );
  T_NewOrder(1   , 2   , 1   , 1           , "info2"     , items1  , 5   );
  T_NewOrder(2   , 1   , 1   , 1           , "info3"     , items1  , 3   );
  T_NewOrder(2   , 10  , 1   , 1           , "info4"     , items2  , 3   );
  T_NewOrder(1   , 2   , 1   , 1           , "info5"     , items2  , 5   );
  T_NewOrder(2   , 1   , 1   , 1           , "info6"     , items2  , 3   );
}

TEST_F(TpccStoredProceduresTest, NewOrder_wrongItemCount) {
  //                     (w_id, d_id, c_id, o_carrier_id, ol_dist_info, {{i_id, i_w_id, quantity}});
  EXPECT_THROW(doNewOrder(1   , 1   , 1   , 1           , "info"      , {{1   , 1     , 1       },
                                                                         {2   , 1     , 1       },
                                                                         {3   , 1     , 1       },
                                                                         {4   , 1     , 1       }}), TpccError); // 4 items

  EXPECT_THROW(doNewOrder(1   , 3   , 2   , 1           , "info"      , {{1   , 1     , 1       },
                                                                         {2   , 1     , 1       },
                                                                         {3   , 1     , 1       },
                                                                         {4   , 1     , 1       },
                                                                         {5   , 1     , 1       },
                                                                         {6   , 1     , 1       },
                                                                         {7   , 1     , 1       },
                                                                         {8   , 1     , 1       },
                                                                         {9   , 1     , 1       },
                                                                         {10  , 1     , 1       },
                                                                         {11  , 1     , 1       },
                                                                         {12  , 1     , 1       },
                                                                         {13  , 1     , 1       },
                                                                         {14  , 1     , 1       },
                                                                         {15  , 1     , 1       },
                                                                         {16  , 1     , 1       }}), TpccError); //16 items
}

TEST_F(TpccStoredProceduresTest, NewOrder_wrongQuantity) {
  //                     (w_id, d_id, c_id, o_carrier_id, ol_dist_info, {{i_id, i_w_id, quantity}});
  EXPECT_THROW(doNewOrder(1   , 1   , 1   , 1           , "info"      , {{1   , 1     , 1       },
                                                                         {2   , 1     , 1       },
                                                                         {3   , 1     , 1       },
                                                                         {4   , 1     , 1       },
                                                                         {5   , 1     , 11      }, }), TpccError);
  
  EXPECT_THROW(doNewOrder(1   , 1   , 5   , 1           , "info"      , {{1   , 1     , 1       },
                                                                         {2   , 1     , 1       },
                                                                         {3   , 1     , 1       },
                                                                         {4   , 1     , 1       },
                                                                         {5   , 1     , 0       }, }), TpccError);
}

TEST_F(TpccStoredProceduresTest, NewOrder_twiceTheSameItem) {
  //                     (w_id, d_id, c_id, o_carrier_id, ol_dist_info, {{i_id, i_w_id, quantity}});
  EXPECT_THROW(doNewOrder(1   , 1   , 1   , 1           , "info"      , {{1   , 1     , 1       },
                                                                         {2   , 1     , 1       },
                                                                         {3   , 1     , 1       },
                                                                         {4   , 1     , 1       },
                                                                         {4   , 1     , 1       }}), TpccError);
}

//===========================Order Status Tests

TEST_F(TpccStoredProceduresTest, OrderStatus) {
  //                           (w_id, d_id, c_id, c_last         );
  auto response = doOrderStatus(1   , 1   , 1   , ""             );
  //                      (w_id, d_id, c_id, c_last         );
  response = doOrderStatus(2   , 3   , 1   , "CLName2"      );
}

TEST_F(TpccStoredProceduresTest, OrderStatus_wrongD_ID) {
  //                        (w_id, d_id, c_id, c_last         );
  EXPECT_THROW(doOrderStatus(1   , 0   , 1   , ""             ), TpccError);
  EXPECT_THROW(doOrderStatus(1   , 11  , 1   , ""             ), TpccError);
  EXPECT_THROW(doOrderStatus(1   , 0   , 1   , "CLName1"      ), TpccError);
  EXPECT_THROW(doOrderStatus(1   , 11  , 1   , "CLName2"      ), TpccError);
}

//============================Payment Tests

TEST_F(TpccStoredProceduresTest, Payment) {
  //                       (w_id, d_id, c_id, c_last        , c_w_id, c_d_id, h_amount, bc_customer);
  auto response = doPayment(1   , 1   , 1   , ""            , 1     , 1     , 300.0f  , true       );
  //                  (w_id, d_id, c_id, c_last        , c_w_id, c_d_id, h_amount, bc_customer);
  response = doPayment(1   , 1   , 1   , "CLName2"     , 1     , 1     , 150.0f  , false      );
}

TEST_F(TpccStoredProceduresTest, Payment_wrongAmount) {
  //                    (w_id, d_id, c_id, c_last        , c_w_id, c_d_id, h_amount, bc_customer);
  EXPECT_THROW(doPayment(1   , 1   , 1   , ""            , 1     , 1     , 99.99f  , true       ), TpccError);
  EXPECT_THROW(doPayment(1   , 1   , 1   , ""            , 1     , 1     , 1000.01f, true       ), TpccError);
}

TEST_F(TpccStoredProceduresTest, Payment_wrongD_ID) {
  //                    (w_id, d_id, c_id, c_last        , c_w_id, c_d_id, h_amount, bc_customer);
  EXPECT_THROW(doPayment(1   , 0   , 1   , ""            , 1     , 1     , 100.0f  , true       ), TpccError);
  EXPECT_THROW(doPayment(1   , 11  , 1   , ""            , 1     , 1     , 100.0f  , true       ), TpccError);
  EXPECT_THROW(doPayment(1   , 1   , 1   , ""            , 1     , 0     , 100.0f  , true       ), TpccError);
  EXPECT_THROW(doPayment(1   , 1   , 1   , ""            , 1     , 11    , 100.0f  , true       ), TpccError);
}


//============================Stock Level Tests

TEST_F(TpccStoredProceduresTest, StockLevel) {
  //                          (w_id, d_id, threshold);
  auto response = doStockLevel(1   , 1   , 20       );

  EXPECT_EQ(1, getValuei(response, "W_ID"));
  EXPECT_EQ(1, getValuei(response, "D_ID"));
  EXPECT_EQ(20, getValuei(response, "threshold"));
  EXPECT_EQ(0, getValuei(response, "low_stock"));

//                       (w_id, d_id, threshold);
  response = doStockLevel(1   , 2   , 20       );

  EXPECT_EQ(1, getValuei(response, "W_ID"));
  EXPECT_EQ(2, getValuei(response, "D_ID"));
  EXPECT_EQ(20, getValuei(response, "threshold"));
  EXPECT_EQ(0, getValuei(response, "low_stock"));

//                       (w_id, d_id, threshold);
  response = doStockLevel(2   , 1   , 20       );

  EXPECT_EQ(2, getValuei(response, "W_ID"));
  EXPECT_EQ(1, getValuei(response, "D_ID"));
  EXPECT_EQ(20, getValuei(response, "threshold"));
  EXPECT_EQ(0, getValuei(response, "low_stock"));

//                       (w_id, d_id, threshold);
  response = doStockLevel(2   , 10  , 20       );

  EXPECT_EQ(2, getValuei(response, "W_ID"));
  EXPECT_EQ(10, getValuei(response, "D_ID"));
  EXPECT_EQ(20, getValuei(response, "threshold"));
  EXPECT_EQ(0, getValuei(response, "low_stock"));
  //TODO 0 values (low_stock) sucks
}

TEST_F(TpccStoredProceduresTest, StockLevel_WrongDistrict) {
  //                       (w_id, d_id, threshold);
  EXPECT_THROW(doStockLevel(1   , 0   , 10       ), TpccError);
  EXPECT_THROW(doStockLevel(1   , 11  , 10       ), TpccError);
}

TEST_F(TpccStoredProceduresTest, StockLevel_WrongThreshold) {
  //                       (w_id, d_id, threshold);
  EXPECT_THROW(doStockLevel(1   , 1   , 9        ), TpccError);
  EXPECT_THROW(doStockLevel(1   , 1   , 21       ), TpccError);
}

} } // namespace hyrise::access

