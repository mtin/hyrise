// Copyright (c) 2012 Hasso-Plattner-Institut fuer Softwaresystemtechnik GmbH. All rights reserved.
#include "access/IntersectPositions.h"

#include "storage/PointerCalculator.h"

namespace hyrise {
namespace access {

namespace { auto _ = QueryParser::registerPlanOperation<IntersectPositions>("IntersectPositions"); }

std::shared_ptr<PlanOperation> IntersectPositions::parse(const Json::Value&) {
  return std::make_shared<IntersectPositions>();
}

void IntersectPositions::executePlanOperation() {

  const auto& pc1 = std::dynamic_pointer_cast<const PointerCalculator>(getInputTable(0));
  if (pc1 == nullptr) { throw std::runtime_error("Passed input 0 is not a PC!"); }
  auto pc3 = pc1;

  for (size_t i=1; i<input.numberOfTables(); ++i) {
    const auto& pc2 = std::dynamic_pointer_cast<const PointerCalculator>(getInputTable(i));
    if (pc2 == nullptr) { throw std::runtime_error("Passed input is not a PC!"); }
    pc3 = pc1->intersect(pc2);
  }
  addResult(pc3);
}

const std::string IntersectPositions::vname() {
  return "IntersectPositions";
};

}
}
