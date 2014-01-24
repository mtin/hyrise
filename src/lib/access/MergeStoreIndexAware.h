// Copyright (c) 2012 Hasso-Plattner-Institut fuer Softwaresystemtechnik GmbH. All rights reserved.
#ifndef SRC_LIB_ACCESS_MERGETABLEINDEXAWARE_H_
#define SRC_LIB_ACCESS_MERGETABLEINDEXAWARE_H_

#include "access/system/PlanOperation.h"

namespace hyrise {
namespace access {

class MergeStoreIndexAware : public PlanOperation {
private:
  std::string _index_name;
  std::string _delta_name;
public:
  virtual ~MergeStoreIndexAware();
  void executePlanOperation();
  static std::shared_ptr<PlanOperation> parse(const Json::Value& data);

  void setIndexName(const std::string &t);
  void setDeltaName(const std::string &t);
};

class MergeStoreIndexAwareBaseline : public PlanOperation {
private:
	std::string _index_name;
public:
  virtual ~MergeStoreIndexAwareBaseline();
  void executePlanOperation();
  static std::shared_ptr<PlanOperation> parse(const Json::Value& data);

  void setIndexName(const std::string &t);
};


}
}

#endif /* SRC_LIB_ACCESS_MERGETABLEINDEXAWARE_H_ */
