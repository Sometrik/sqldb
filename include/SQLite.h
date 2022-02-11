#ifndef _SQLDB_SQLITE_H_
#define _SQLDB_SQLITE_H_

#include "Connection.h"
#include "SQLStatement.h"

#include <sqlite3.h>

namespace sqldb {
  class SQLite : public Connection {
  public:
    SQLite(const std::string & db_file, bool read_only = false);
    ~SQLite();
  
    std::shared_ptr<sqldb::SQLStatement> prepare(const std::string & query) override;
    bool isConnected() const { return true; }
    
  private:
    bool open(bool read_only);
  
    std::string db_file_;
    sqlite3 * db_;  
  };
};

#endif
