#include "testing/test.h"
#include "io/BufferedLogger.h"
#include "io/shortcuts.h"
#include "io/StorageManager.h"
#include "storage/Store.h"
#include "io/TransactionManager.h"
#include "access/InsertScan.h"
#include "access/tx/Commit.h"

#include <fstream>
#include <thread>
#include <vector>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <sys/mman.h>

#include <helper/Settings.h>

namespace hyrise {
namespace io {

class BufferedLoggerTests : public ::hyrise::Test {};

TEST_F(BufferedLoggerTests, simple_log_test) {
  uint64_t tx = 17;
  std::string table_name("Tabelle");

  BufferedLogger::getInstance().logDictionary<int64_t>(table_name, 1, 2, 31);
  BufferedLogger::getInstance().logDictionary<float>(table_name, 2, 2.0f, 42);
  BufferedLogger::getInstance().logDictionary<std::string>(table_name, 3, "zwei", 53);
  std::vector<ValueId> vids;
  vids.push_back(ValueId(31, 1));
  vids.push_back(ValueId(42, 2));
  vids.push_back(ValueId(53, 3));
  BufferedLogger::getInstance().logValue(tx, table_name, 3, 2, (1<<3)-1, &vids);
  BufferedLogger::getInstance().logCommit(tx);
  BufferedLogger::getInstance().flush();

  int log_fd = open((Settings::getInstance()->getDBPath() + "/log/log.bin").c_str(), O_RDONLY);
  int reference_fd = open((Settings::getInstance()->getDBPath() + "/buffered_logger_logfile.bin").c_str(), O_RDONLY);
  ASSERT_TRUE(log_fd);
  ASSERT_TRUE(reference_fd);

  struct stat log_s, reference_s;
  fstat(log_fd, &log_s);
  fstat(reference_fd, &reference_s);
  ASSERT_EQ(log_s.st_size, reference_s.st_size);

  auto log = (char*)mmap(NULL, log_s.st_size, PROT_READ, MAP_PRIVATE, log_fd, 0);
  auto reference = (char*)mmap(NULL, reference_s.st_size, PROT_READ, MAP_PRIVATE, reference_fd, 0);

  ASSERT_FALSE(memcmp(log, reference, log_s.st_size));
}

TEST_F(BufferedLoggerTests, simple_restore_test) {
  BufferedLogger::getInstance().restore((Settings::getInstance()->getDBPath() + "/buffered_logger_logfile.bin").c_str());
}

TEST_F(BufferedLoggerTests, insert_and_restore_test) {
  auto rows = Loader::shortcuts::load("test/alltypes.tbl");
  auto empty = Loader::shortcuts::load("test/alltypes_empty.tbl");

  storage::atable_ptr_t orig(new storage::Store(empty));
  orig->setName("TABELLE");
  StorageManager::getInstance()->add("TABELLE", orig);
  #ifdef PERSISTENCY_BUFFEREDLOGGER
    StorageManager::getInstance()->persistTable("TABELLE");
  #endif

  auto ctx = tx::TransactionManager::getInstance().buildContext();

  access::InsertScan is;
  is.setTXContext(ctx);
  is.addInput(orig);
  is.setInputData(rows);
  is.execute();

  access::Commit c;
  c.addInput(orig);
  c.setTXContext(ctx);
  c.execute();

  StorageManager::getInstance()->removeTable("TABELLE");

  storage::atable_ptr_t restored(new storage::Store(rows->copy_structure_modifiable()));
  StorageManager::getInstance()->add("TABELLE", restored);

  BufferedLogger::getInstance().restore();

  ASSERT_TABLE_EQUAL(orig, restored);
}

}
}
