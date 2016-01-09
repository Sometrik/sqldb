#ifndef _SQLDB_CONNECTION_H_
#define _SQLDB_CONNECTION_H_

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
    virtual void begin();
    virtual void commit();
    virtual void rollback();
    virtual unsigned int execute(const char * query);
    virtual bool ping() { return true; }    
    
    unsigned int execute(const std::string & query) { return execute(query.c_str()); }
  };
};

#endif
