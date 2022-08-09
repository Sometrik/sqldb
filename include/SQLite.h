#ifndef _SQLDB_SQLITE_H_
#define _SQLDB_SQLITE_H_

#include "Connection.h"
#include "SQLStatement.h"

#include <sqlite3.h>

namespace sqldb {
  class SQLite : public Connection {
  public:
    SQLite(const std::string & db_file, bool read_only = false);
    SQLite(const SQLite & other);
    SQLite(SQLite && other);
    ~SQLite();
  
    std::unique_ptr<sqldb::SQLStatement> prepare(std::string_view query) override;
    bool isConnected() const { return true; }
    
  private:
    bool open();
  
    std::string db_file_;
    sqlite3 * db_;
    bool read_only_;
  };
};

#endif
