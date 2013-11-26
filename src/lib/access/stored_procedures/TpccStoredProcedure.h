// Copyright (c) 2013 Hasso-Plattner-Institut fuer Softwaresystemtechnik GmbH. All rights reserved.
#pragma once

#include "json.h"

#include <helper/types.h>
#include <io/TXContext.h>
#include <access/expressions/pred_SimpleExpression.h>
#include "access/system/ResponseTask.h"
#include <net/Router.h>
#include <net/AbstractConnection.h>
#include <storage/Store.h>
#include <map>

namespace hyrise { namespace access {

class TpccStoredProcedure : public net::AbstractRequestHandler {
 public:
  TpccStoredProcedure(net::AbstractConnection* connection);
  virtual ~TpccStoredProcedure(){}

  void operator()();
  virtual Json::Value execute() = 0;
  virtual void setData(const Json::Value& data) = 0;

 protected:
  //connection helper
  Json::Value data();

 protected:
  //json helper
  static Json::Value assureMemberExists(const Json::Value& data, const std::string& name);
  static int assureIntValueBetween(const Json::Value& data, const std::string& name, int min, int max);
  static float assureFloatValueBetween(const Json::Value& data, const std::string& name, float min, float max);

  void startTransaction();

  //planop helper
  storage::c_store_ptr_t getTpccTable(std::string name) ;
  storage::c_atable_ptr_t selectAndValidate(storage::c_atable_ptr_t table, std::string tablename, std::unique_ptr<AbstractExpression> expr) const;
  storage::atable_ptr_t newRowFrom(storage::c_atable_ptr_t table) const;
  void insert(storage::atable_ptr_t table, storage::atable_ptr_t rows) const;
  void deleteRows(storage::c_atable_ptr_t rows) const;
  void update(storage::c_atable_ptr_t rows, const Json::Value& data) const;
  storage::c_atable_ptr_t sort(storage::c_atable_ptr_t table, field_name_t field, bool ascending) const;

  typedef std::vector<SimpleExpression*> expr_list_t;
  static std::unique_ptr<AbstractExpression> connectAnd(expr_list_t expressions);

  //transaction
  void commit();
  void rollback();

  static std::string getDate();

  net::AbstractConnection* _connection;
  tx::TXContext _tx;
  std::shared_ptr<ResponseTask> _responseTask;

 private:
  bool _finished = false;
  bool _recordPerformance = false;
  std::map<std::string, storage::c_store_ptr_t> _tables;

  #ifdef WITH_GROUP_COMMIT
  bool _use_group_commit = true;
  #else
  bool _use_group_commit = false;
  #endif
};

} } // namespace hyrise::access

