#ifndef SRC_LIB_IO_BUFFEREDLOGGER_H
#define SRC_LIB_IO_BUFFEREDLOGGER_H

#include <atomic>
#include <cstdio>
#include <cstring>
#include <mutex>

#include "storage/storage_types.h"
#include "helper/types.h"

namespace hyrise {
namespace io {

class BufferedLogger {
public:
    BufferedLogger(const BufferedLogger&) = delete;
    BufferedLogger &operator=(const BufferedLogger&) = delete;

    static BufferedLogger &getInstance();

    template<typename T>
    void logDictionary(const std::string& table_name,
                                          storage::field_t column,
                                          const T &value,
                                          storage::value_id_t value_id) {
      char entry[90];
      char *cursor = entry;

      *cursor = 'D';
      cursor += sizeof(char);

      assert(table_name.size() > 0);
      *(char*) cursor = table_name.size();
      cursor += sizeof(char);

      memcpy(cursor, table_name.c_str(), table_name.size());
      cursor += table_name.size();

      *(storage::field_t*)cursor = column;
      cursor += sizeof(column);

      *(storage::value_id_t*)cursor = value_id;
      cursor += sizeof(value_id);

      logDictionaryValue(cursor, value);

      unsigned int len = cursor - entry;
      assert(len <= 90);
      _append(entry, len);
    }

    template<typename T>
    void logDictionaryValue(char *&cursor, const T &value) {
      // used for all data types where their size is sizeof(T)

      *(int*)cursor = sizeof(value);
      cursor += sizeof(int);

      *(T*)cursor = value;
      cursor += sizeof(value);
    }

    void logValue(tx::transaction_id_t transaction_id,
                  const std::string& table_name,
                  storage::pos_t row,
                  storage::pos_t invalidated_row,
                  uint64_t field_bitmask,
                  const ValueIdList *value_ids);
    void logCommit(tx::transaction_id_t transaction_id);
    void flush();
    void restore(const char* logfile = NULL);

private:
    BufferedLogger();

    void _append(const char *str, const unsigned int len);

    FILE *_logfile;
    char *_buffer;
    char *_head;
    char *_tail;
    char *_last_write;
    uint64_t _buffer_size;

    std::mutex _bufferMutex;
    std::mutex _fileMutex;
    std::atomic<int> _writing;
    std::atomic<uint64_t> _size;
};

template<> void BufferedLogger::logDictionaryValue(char *&cursor, const storage::hyrise_string_t &value);

}
}

#endif // SRC_LIB_IO_BUFFEREDLOGGER_H
