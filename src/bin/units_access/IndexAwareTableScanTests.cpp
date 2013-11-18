// Copyright (c) 2012 Hasso-Plattner-Institut fuer Softwaresystemtechnik GmbH. All rights reserved.
#include "access/IndexAwareTableScan.h"
#include "access/CreateGroupkeyIndex.h"
#include "helper/types.h"
#include "io/shortcuts.h"
#include "testing/test.h"
#include "helper/checked_cast.h"
#include "access/InsertScan.h"
#include "storage/Store.h"
#include "io/TransactionManager.h"
#include "access/expressions/pred_LessThanExpression.h"
#include "testing/TableEqualityTest.h"

namespace hyrise {
namespace access {

class IndexAwareTableScanTests : public AccessTest {
public:
  IndexAwareTableScanTests() {}

  virtual void SetUp() {
    AccessTest::SetUp();
    t = Loader::shortcuts::load("test/index_test.tbl");
    CreateGroupkeyIndex ci;
    ci.addInput(t);
    ci.addField(0);
    ci.setIndexName("idx__foo__col_0");
    ci.execute();

    auto row = Loader::shortcuts::load("test/index_insert_test.tbl");
    storage::atable_ptr_t table(new storage::Store(row));
    auto ctx = tx::TransactionManager::getInstance().buildContext();
    InsertScan ins;
    ins.setTXContext(ctx);
    ins.addInput(t);
    ins.setInputData(row);
    ins.execute();
  }

  storage::atable_ptr_t t;
};

TEST_F(IndexAwareTableScanTests, basic_index_aware_table_scan_test_eq) {
  auto reference = Loader::shortcuts::load("test/reference/index_aware_test_result.tbl");

  IndexAwareTableScan is;
  is.addInput(t);
  is.setTableName("foo");
  is.addField("col_0");
  is.setPredicate(new GenericExpressionValue<hyrise_int_t, std::equal_to<hyrise_int_t>>(0, "col_0", 200));
  is.execute();
  auto result = is.getResultTable();
  ASSERT_TABLE_EQUAL(result, reference);
}

TEST_F(IndexAwareTableScanTests, basic_index_aware_table_scan_test_lt) {
  auto reference = Loader::shortcuts::load("test/reference/index_aware_test_result_lt.tbl");

  IndexAwareTableScan is;
  is.addInput(t);
  is.setTableName("foo");
  is.addField("col_0");
  is.setPredicate(new GenericExpressionValue<hyrise_int_t, std::less<hyrise_int_t>>(0, "col_0", 200));
  is.execute();
  auto result = is.getResultTable();
  EXPECT_RELATION_EQ(result, reference);
}

} // namespace access
} // namespace hyrise
