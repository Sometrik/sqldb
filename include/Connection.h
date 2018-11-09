#ifndef _SQLDB_CONNECTION_H_
#define _SQLDB_CONNECTION_H_

#include <SQLStatement.h>

#include <string>
#include <memory>

namespace sqldb {
  class SQLStatement;
  
  class Connection {
  public:
    Connection() { }
    Connection(const Connection & other) = delete;
    virtual ~Connection() { }
    Connection & operator=(const Connection & other) = delete;
    
    virtual std::shared_ptr<sqldb::SQLStatement> prepare(const std::string & query) = 0;
    virtual void begin() {
      execute("BEGIN TRANSACTION");
    }
    virtual void commit() {
      execute("COMMIT");
    }
    virtual void rollback() {
      execute("ROLLBACK");
    }
    virtual size_t execute(const char * query) {
      auto stmt = prepare(query);
      return stmt->execute();
    }
    virtual bool ping() { return true; }    
    
    size_t execute(const std::string & query) { return execute(query.c_str()); }
  };
};

#endif
