// Copyright (c) 2012 Hasso-Plattner-Institut fuer Softwaresystemtechnik GmbH. All rights reserved.
#ifndef SRC_LIB_ACCESS_MERGETABLEINDEXAWARE_H_
#define SRC_LIB_ACCESS_MERGETABLEINDEXAWARE_H_

#include "access/system/PlanOperation.h"

namespace hyrise {
namespace access {

class MergeTableIndexAware : public PlanOperation {
private:
  std::string _index_name;
public:
  virtual ~MergeTableIndexAware();
  void executePlanOperation();
  static std::shared_ptr<PlanOperation> parse(const Json::Value& data);

  void setIndexName(const std::string &t);
};

class MergeTableIndexAwareBaseline : public PlanOperation {
private:
	std::string _index_name;
public:
  virtual ~MergeTableIndexAwareBaseline();
  void executePlanOperation();
  static std::shared_ptr<PlanOperation> parse(const Json::Value& data);

  void setIndexName(const std::string &t);
};


}
}

#endif /* SRC_LIB_ACCESS_MERGETABLEINDEXAWARE_H_ */
