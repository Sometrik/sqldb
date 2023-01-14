#ifndef _SQLDB_MYSQL_H_
#define _SQLDB_MYSQL_H_

#include "Connection.h"

#include <mysql.h>

namespace sqldb {
  class MySQL : public Connection {
  public:
    MySQL() { }
    ~MySQL();
    
    void connect(std::string host_name, int port, std::string user_name, std::string password, std::string db_name);
    void connect();
    
    std::unique_ptr<SQLStatement> prepare(std::string_view query) override;
    bool ping() override;
    void begin() override;
    void commit() override;
    void rollback() override;

    size_t execute(std::string_view query) override;

    bool isConnected() const { return conn_ != 0; }

  private:
    MYSQL * conn_ = 0;
    std::string host_name_, user_name_, password_, db_name_;
    int port_ = 0;
  };
};

#endif
