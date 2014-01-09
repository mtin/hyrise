/*
 * Logging Format:
 *
 * Every entry has the structure Type + Data
 * Type distinguishs between dictionary entries ("D"), value entries ("V"), and commits ("C")
 * Dictionary entries have the format "D" + table_name.size() + table_name + field_id + value_id + (int)len(value) + value
 * Value entries have the format "V" + transaction_id + table_name.size() + table_name + row_id + invalidated_row_id + field_bitmask (+ value_id)*
 * Commit entries have the format "C" + transaction_id

 * Entries are not separated. Instead, their length is implicitly or explicitly known. After identifying the type of an entry,
 * the next entry can be found by adding the size of the entry
 *     Dictionary Entries are sizeof(char) + sizeof(char) + table_name.size() + sizeof(field_id_t) + sizeof(value_id_t) + sizeof(int) + len(value)
 *     Value Entries are sizeof(char) + sizeof(transaction_id_t) + sizeof(char) + table_name.size() + sizeof(pos_t) + sizeof(pos_t) + sizeof(size_t) + popcount(field_bitmask)
 *     Commit Entries are sizeof(transaction_id_t)
 */


#include "io/BufferedLogger.h"
#include "io/StorageManager.h"
#include "io/TransactionManager.h"
#include "storage/Store.h"
#include "storage/OrderIndifferentDictionary.h"

#include <unistd.h>
#include <errno.h>
#include <sys/types.h>
#include <dirent.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <sys/mman.h>

#include <helper/Settings.h>
#include <helper/types.h>

namespace hyrise {
namespace io {

constexpr uint64_t LOG_BUFFER_SIZE = 16384;
//constexpr char LOG_DIR_NAME[] = ".";
//constexpr char LOG_FILE_NAME[] = (Settings::getInstance()->getDBPath() + "/log/").c_str(); // must include directory

BufferedLogger &BufferedLogger::getInstance() {
  static BufferedLogger instance;
  return instance;
}

template<>
void BufferedLogger::logDictionaryValue(char *&cursor, const storage::hyrise_string_t &value) {
  *(int*)cursor = value.size();
  cursor += sizeof(int);

  memcpy(cursor, value.c_str(), value.size());
  cursor += value.size();
}

void BufferedLogger::logValue(const tx::transaction_id_t transaction_id,
                              const std::string& table_name,
                              const storage::pos_t row,
                              const storage::pos_t invalidated_row,
                              const uint64_t field_bitmask,
                              const ValueIdList *value_ids) {
  char entry[200];
  char *cursor = entry;

  *cursor = 'V';
  cursor += sizeof(char);

  *(tx::transaction_id_t*)cursor = transaction_id;
  cursor += sizeof(transaction_id);

  assert(table_name.size() > 0);
  *(char*) cursor = table_name.size();
  cursor += sizeof(char);

  memcpy(cursor, table_name.c_str(), table_name.size());
  cursor += table_name.size();

  *(storage::pos_t*)cursor = row;
  cursor += sizeof(row);

  *(storage::pos_t*)cursor = invalidated_row;
  cursor += sizeof(invalidated_row);

  *(uint64_t*)cursor = field_bitmask;
  cursor += sizeof(field_bitmask);

  if(value_ids != nullptr) {
    for(auto it = value_ids->cbegin(); it != value_ids->cend(); ++it) {
      *(storage::value_id_t*)cursor = it->valueId;
      cursor += sizeof(it->valueId);
    }
  }

  size_t len = cursor - entry;
  assert(len <= 200);
  _append(entry, len);
}

void BufferedLogger::logCommit(const tx::transaction_id_t transaction_id) {
  char entry[9];
  entry[0] = 'C';
  *(tx::transaction_id_t*)(entry+1) = transaction_id;
  _append(entry, 9);
}

void BufferedLogger::_append(const char *str, const unsigned int len) {
  char *head = NULL;

  _bufferMutex.lock();
  head = _head;
  _head = _buffer + ((_head - _buffer + len) % LOG_BUFFER_SIZE);
  ++_writing;
  _bufferMutex.unlock();

  if(head + len < _tail) {
    memcpy(head, str, len);
  } else {
    uint64_t part1 = _tail - head;
    uint64_t part2 = len - part1;
    memcpy(head, str, part1);
    memcpy(_buffer, str+part1, part2);
  }

  --_writing;

  uint64_t s = _size.fetch_add(len);
  if(s > LOG_BUFFER_SIZE/2) flush();
}

void BufferedLogger::flush() {
  char *head = NULL;
  uint64_t written = 0;

  _fileMutex.lock();

  _bufferMutex.lock();
  while(_writing > 0);
  head = _head;
  _bufferMutex.unlock();

  if(head > _last_write) {
    written = head - _last_write;
    fwrite(_last_write, sizeof(char), written, _logfile);
  } else if(head < _last_write) {
    uint64_t part1 = _tail - _last_write;
    uint64_t part2 = head - _buffer;
    written = part1 + part2;
    fwrite(_last_write, sizeof(char), part1, _logfile);
    fwrite(_buffer, sizeof(char), part2, _logfile);
  } else {
    _fileMutex.unlock();
    return;
  }
  _size -= written;
  _last_write = head;

  #ifndef COMMIT_WITHOUT_FLUSH
  if (fflush(_logfile) != 0) {
    printf( "Something went wrong while flushing the logfile: %s\n", strerror( errno ) );
  }
  if (fsync(fileno(_logfile)) != 0) {
    printf( "Something went wrong while flushing the logfile: %s\n", strerror( errno ) );
  }
  #endif

  _fileMutex.unlock();
}

BufferedLogger::BufferedLogger() {
  _logfile = fopen((Settings::getInstance()->getDBPath() + "/log/log.bin").c_str(), "w");
  fsync(fileno(_logfile));
  fsync(dirfd(opendir((Settings::getInstance()->getDBPath() + "/log/").c_str())));

  _buffer_size = LOG_BUFFER_SIZE;
  _buffer = (char*) malloc(_buffer_size);
  _head = _buffer;
  _last_write = _buffer;
  _tail = _buffer + _buffer_size;
  _writing = 0;
  _size = 0;
}

void BufferedLogger::truncate() {
  fclose(_logfile);
  _logfile = fopen((Settings::getInstance()->getDBPath() + "/log/log.bin").c_str(), "w");
  fsync(fileno(_logfile));
  fsync(dirfd(opendir((Settings::getInstance()->getDBPath() + "/log/").c_str())));
  _head = _buffer;
  _last_write = _buffer;
  _writing = 0;
  _size = 0;
}

void BufferedLogger::restore() {
  int fd = open((Settings::getInstance()->getDBPath() + "/log/log.bin").c_str(), O_RDWR);
  assert(fd);

  struct stat s;
  fstat(fd, &s);

  auto log = (char*)mmap(0, s.st_size, PROT_READ, MAP_PRIVATE, fd, 0);

  std::vector<tx::TXModifications> tx_data;
  tx::transaction_cid_t last_cid = 0; // FIXME - this reuses CIDs that were already used in the main

  char *cursor = log;
  while(cursor < log + s.st_size) {
    char entry_type = *(char*)cursor;
    cursor += sizeof(entry_type);
    switch(entry_type) {
      case 'D': {
        auto table_name_size = *(char*)cursor;
        cursor += sizeof(table_name_size);

        std::string table_name(cursor, table_name_size);
        cursor += table_name_size;

        auto column = *(storage::field_t*)cursor;
        cursor += sizeof(column);

        auto value_id = *(storage::value_id_t*)cursor;
        cursor += sizeof(value_id);

        auto value_size = *(int*)cursor;
        cursor += sizeof(value_size);

        auto store = std::dynamic_pointer_cast<storage::Store>(StorageManager::getInstance()->getTable(table_name));
        switch(store->typeOfColumn(column)) {
          // kill me...
          case IntegerType: {
            auto dict = std::dynamic_pointer_cast<OrderIndifferentDictionary<storage::hyrise_int_t>>(store->getDeltaTable()->dictionaryAt(column));
            assert(dict);
            auto value = *(storage::hyrise_int_t*)cursor;
            dict->setValue(value, value_id);
            break;
          }
          case FloatType: {
            auto dict = std::dynamic_pointer_cast<OrderIndifferentDictionary<storage::hyrise_float_t>>(store->getDeltaTable()->dictionaryAt(column));
            assert(dict);
            auto value = *(storage::hyrise_float_t*)cursor;
            dict->setValue(value, value_id);
            break;
          }
          case StringType: {
            auto dict = std::dynamic_pointer_cast<OrderIndifferentDictionary<storage::hyrise_string_t>>(store->getDeltaTable()->dictionaryAt(column));
            assert(dict);
            auto value = storage::hyrise_string_t(cursor, value_size);
            dict->setValue(value, value_id);
            break;
          }
        }
        cursor += value_size;

        break;
      }

      case 'V': {
        auto transaction_id = *(tx::transaction_id_t*)cursor;
        if(transaction_id >= (int64_t)tx_data.size()) tx_data.resize(transaction_id + 1);
        cursor += sizeof(transaction_id);

        auto table_name_size = *(char*)cursor;
        cursor += sizeof(table_name_size);

        std::string table_name(cursor, table_name_size);
        auto store = std::dynamic_pointer_cast<storage::Store>(StorageManager::getInstance()->getTable(table_name));
        cursor += table_name_size;

        auto row = *(storage::pos_t*)cursor;
        cursor += sizeof(row);

        auto invalidated_row = *(storage::pos_t*)cursor;
        if(invalidated_row) {
          store->markForDeletion(row, transaction_id);
          tx_data[transaction_id].deletePos(store, row);
        }
        cursor += sizeof(invalidated_row);

        auto field_bitmask = *(uint64_t*)cursor;
        cursor += sizeof(field_bitmask);

        if(field_bitmask) {
          // insert or update
          auto pos_in_delta = row - store->deltaOffset();
          if(row >= store->size()) store->appendToDelta(pos_in_delta - store->size() + 1);
          for(int i = 0; i < __builtin_popcount(field_bitmask); i++) {
            // currently, we always log all values when updating
            // if this is to change, we will have to find the positions of the set bits
            auto value_id = *(storage::value_id_t*)cursor;
            store->getDeltaTable()->setValueId(i, pos_in_delta, ValueId(value_id, 0));
            cursor += sizeof(value_id);
          }
          store->addRowToDeltaIndices(row);
          tx_data[transaction_id].insertPos(store, row);
        } else {
          // delete was handled before
        }

        break;
      }

      case 'C': {
        auto transaction_id = *(tx::transaction_id_t*)cursor;
        cursor += sizeof(transaction_id);

        if(transaction_id >= (int64_t)tx_data.size()) break;

        auto modifications = tx_data[transaction_id];

        for(auto& kv: modifications.inserted) {
          auto weak_table = kv.first;
          if (auto store = std::const_pointer_cast<storage::Store>(std::dynamic_pointer_cast<const storage::Store>(weak_table.lock()))) {
            store->commitPositions(kv.second, last_cid+1, true);
          }
        }

        for(auto& kv: modifications.deleted) {
          auto weak_table = kv.first;
          if (auto store = std::const_pointer_cast<storage::Store>(std::dynamic_pointer_cast<const storage::Store>(weak_table.lock()))) {
            store->commitPositions(kv.second, last_cid+1, false);
          }
        }

        last_cid++;

        break;
      }
    }
  }
}

}
}
