// Copyright (c) 2013 Hasso-Plattner-Institut fuer Softwaresystemtechnik GmbH. All rights reserved.

#include "TpccStoredProcedure.h"

#include <ctime>

#include <storage/AbstractTable.h>
#include <storage/TableBuilder.h>
#include <storage/storage_types_helper.h>
#include <helper/HttpHelper.h>

#include <io/ResourceManager.h>
#include <io/TransactionManager.h>
#include <access.h>
#include "access/storage/GetTable.h"


namespace hyrise { namespace access {

TpccStoredProcedure::TpccStoredProcedure(net::AbstractConnection* connection) :
  _connection(connection),
  _responseTask(std::make_shared<ResponseTask>(connection)) {

   _responseTask->setRecordPerformanceData(true);
}

void TpccStoredProcedure::operator()() {
  // bool successful_commit = false;

  try {
    auto d = data();
    setData(d);
  
    startTransaction();
    Json::Value result;
    result = execute();    
    
    if (_recordPerformance) {
      auto json_result_with_perf = _responseTask->generateResponseJson();
      result["performanceData"] = json_result_with_perf["performanceData"];
    }

    Json::StyledWriter writer;
    _connection->respond(writer.write(result));

  }
  catch (std::runtime_error e) {
    // if (!successful_commit)
    rollback();
    _connection->respond(std::string("error: ") + e.what());
    std::cout << std::string("error: ") + e.what();
    return; 
  }
}

Json::Value TpccStoredProcedure::data() {
  if (!_connection->hasBody())
    throw std::runtime_error("message has no body");

  std::map<std::string, std::string> body_data = parseHTTPFormData(_connection->getBody());
  auto data = body_data.find("query");

  if (data == body_data.end()) {
    throw std::runtime_error("No data object in json");
  }
  auto request = urldecode(body_data["query"]);

  _recordPerformance = getOrDefault(body_data, "performance", "false") == "true";

  Json::Value request_data;
  Json::Reader reader;

  if (!reader.parse(request, request_data)) {
    throw std::runtime_error("Failed to parse json");
  }

  return request_data;
}

namespace {
  typedef std::map<std::string, std::string> file_map_t;

  static const std::string tpccTableDir = "test/tpcc/";
  static const std::string tpccDelimiter = ",";
  const file_map_t tpccHeaderLocation = {{"DISTRICT"  , tpccTableDir + "district_header.tbl"},
                                         {"WAREHOUSE" , tpccTableDir + "warehouse_header.tbl"},
                                         {"CUSTOMER"  , tpccTableDir + "customer_header.tbl"},
                                         {"HISTORY"   , tpccTableDir + "history_header.tbl"},
                                         {"ORDERS"    , tpccTableDir + "orders_header.tbl"},
                                         {"NEW_ORDER" , tpccTableDir + "new_order_header.tbl"},
                                         {"STOCK"     , tpccTableDir + "stock_header.tbl"},
                                         {"ORDER_LINE", tpccTableDir + "order_line_header.tbl"},
                                         {"ITEM"      , tpccTableDir + "item_header.tbl"}};
  const file_map_t tpccDataLocation = {{"DISTRICT"  , tpccTableDir + "district.csv"},
                                       {"WAREHOUSE" , tpccTableDir + "warehouse.csv"},
                                       {"CUSTOMER"  , tpccTableDir + "customer.csv"},
                                       {"HISTORY"   , tpccTableDir + "history.csv"},
                                       {"ORDERS"    , tpccTableDir + "orders.csv"},
                                       {"NEW_ORDER" , tpccTableDir + "new_order.csv"},
                                       {"STOCK"     , tpccTableDir + "stock.csv"},
                                       {"ORDER_LINE", tpccTableDir + "order_line.csv"},
                                       {"ITEM"      , tpccTableDir + "item.csv"}};
} // namespace

Json::Value TpccStoredProcedure::assureMemberExists(const Json::Value& data, const std::string& name) {
  if (!data.isMember(name))
    throw std::runtime_error("Parameter \"" + name + "\" is not defined");
  return data[name];
}

int TpccStoredProcedure::assureIntValueBetween(const Json::Value& data, const std::string& name, int min, int max) {
  const int value = assureMemberExists(data, name).asInt();
  if (value < min || value > max) {
    std::ostringstream os;
    os << "\"" << name << "\" must contain an integer value between " << min << " and " << max;
    throw std::runtime_error(os.str());
  }
  return value;
}

float TpccStoredProcedure::assureFloatValueBetween(const Json::Value& data, const std::string& name, float min, float max) {
  const float value = assureMemberExists(data, name).asFloat();
  if (value < min || value > max) {
    std::ostringstream os;
    os << "\"" << name << "\" must contain a floating point value between " << min << " and " << max;
    throw std::runtime_error(os.str());
  }
  return value;
}
storage::c_store_ptr_t TpccStoredProcedure::getTpccTable(std::string name) const {
  // TableLoad load;

  std::shared_ptr<GetTable> load = std::make_shared<GetTable>(name);
  load->setOperatorId("__GetTable");
  load->setPlanOperationName("GetTable");
  _responseTask->registerPlanOperation(load);

  load->setTXContext(_tx);
  load->execute();

  storage::c_atable_ptr_t table = load->getResultTable();
  storage::c_store_ptr_t store = std::dynamic_pointer_cast<const storage::Store>(table);
  if(!store) throw std::runtime_error(name + " is not a Store");

  return store;
}

storage::c_atable_ptr_t TpccStoredProcedure::selectAndValidate(storage::c_atable_ptr_t table, std::string tablename, std::unique_ptr<AbstractExpression> expr) const {
  
  std::shared_ptr<IndexAwareTableScan> select = std::make_shared<IndexAwareTableScan>();
  select->setOperatorId("__IndexAwareTableScan");
  select->setPlanOperationName("IndexAwareTableScan");
  _responseTask->registerPlanOperation(select);

  select->addInput(table);
  select->setTXContext(_tx);
  select->setTableName(tablename);
  SimpleExpression* bare = dynamic_cast<SimpleExpression*>(expr.release()); // Predicates get deleted by operator
  select->setPredicate(bare);
  select->execute();

  std::shared_ptr<ValidatePositions> validate = std::make_shared<ValidatePositions>();
  validate->setOperatorId("__ValidatePositions");
  validate->setPlanOperationName("ValidatePositions");
  _responseTask->registerPlanOperation(validate);

  validate->setTXContext(_tx);
  validate->addInput(select->getResultTable());
  validate->execute();

  return validate->getResultTable();
}

void TpccStoredProcedure::startTransaction() {
    _tx = tx::TransactionManager::beginTransaction();
    _responseTask->setTxContext(_tx);
}

storage::atable_ptr_t TpccStoredProcedure::newRowFrom(storage::c_atable_ptr_t table) const {
  const auto metadata = table->metadata();
  storage::TableBuilder::param_list list;
  for (const auto& columnData : metadata) {
    list.append(storage::TableBuilder::param(columnData.getName(), data_type_to_string(columnData.getType())));
  }
  auto newRow = storage::TableBuilder::build(list);
  newRow->resize(1); //TODO it's halloween for now but there should be a more generic value

  return newRow;
}

void TpccStoredProcedure::insert(storage::atable_ptr_t table, storage::atable_ptr_t rows) const {
  
  std::shared_ptr<InsertScan> insert = std::make_shared<InsertScan>();
  insert->setOperatorId("__InsertScan");
  insert->setPlanOperationName("InsertScan");
  _responseTask->registerPlanOperation(insert);

  insert->setTXContext(_tx);
  insert->addInput(table);
  insert->setInputData(rows);
  insert->execute();
}

void TpccStoredProcedure::deleteRows(storage::c_atable_ptr_t rows) const {

  std::shared_ptr<DeleteOp> del = std::make_shared<DeleteOp>();
  del->setOperatorId("__DeleteOp");
  del->setPlanOperationName("DeleteOp");
  _responseTask->registerPlanOperation(del);

  del->setTXContext(_tx);
  del->addInput(rows);
  del->execute();
}

void TpccStoredProcedure::update(storage::c_atable_ptr_t rows, const Json::Value& data) const {
  std::shared_ptr<PosUpdateScan> update = std::make_shared<PosUpdateScan>();
  update->setOperatorId("__PosUpdateScan");
  update->setPlanOperationName("PosUpdateScan");
  _responseTask->registerPlanOperation(update);

  update->setTXContext(_tx);
  update->addInput(rows);
  update->setRawData(data);
  update->execute();
}

storage::c_atable_ptr_t TpccStoredProcedure::sort(storage::c_atable_ptr_t table, field_name_t field, bool ascending) const {
  std::shared_ptr<SortScan> sort = std::make_shared<SortScan>();
  sort->setOperatorId("__SortScan");
  sort->setPlanOperationName("SortScan");
  _responseTask->registerPlanOperation(sort);

  sort->setTXContext(_tx);
  sort->addInput(table);
  sort->setSortField(field);
  sort->execute();
  return sort->getResultTable();
}

std::unique_ptr<AbstractExpression> TpccStoredProcedure::connectAnd(expr_list_t expressions) {
  if (expressions.size() == 0)
    throw std::runtime_error("cannot connect no (0) expressions");
  else if (expressions.size() == 1)
    throw std::runtime_error("AND with one expression makes no sense");

  auto lastAnd = new CompoundExpression(expressions.at(0), expressions.at(1), AND);
  for (size_t i = 2; i < expressions.size(); ++i)
    lastAnd = new CompoundExpression(expressions.at(i), lastAnd, AND);
  return std::unique_ptr<AbstractExpression>(lastAnd);
}

void TpccStoredProcedure::commit() {
  if (_finished)
    throw std::runtime_error("cannot commit twice");
  
  std::shared_ptr<Commit> commit = std::make_shared<Commit>();
  commit->setOperatorId("__Commit");
  commit->setPlanOperationName("Commit");
  _responseTask->registerPlanOperation(commit);

  commit->setTXContext(_tx);
  commit->execute();
  _finished = true;
}

void TpccStoredProcedure::rollback() {
  if (_finished)
    throw std::runtime_error("cannot rollback twice");
  
  std::shared_ptr<Rollback> rb = std::make_shared<Rollback>();
  rb->setOperatorId("__Rollback");
  rb->setPlanOperationName("Rollback");
  _responseTask->registerPlanOperation(rb);

  rb->setTXContext(_tx);
  rb->execute();
  _finished = true;
}

//source http://stackoverflow.com/questions/5438482
// Get current date/time, format is YYYY-MM-DD
std::string TpccStoredProcedure::getDate() {
    time_t     now = time(0);
    struct tm  tstruct;
    char       buf[11];
    tstruct = *localtime(&now);
    strftime(buf, sizeof(buf), "%Y-%m-%d", &tstruct);
    return buf;
}

} } // namespace hyrise::access

