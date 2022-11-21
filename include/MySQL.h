#ifndef _SQLDB_MYSQL_H_
#define _SQLDB_MYSQL_H_

#include "Connection.h"

#include <mysql.h>

namespace sqldb {
  class MySQL : public Connection {
  public:
    MySQL() { }
    ~MySQL();
    
    void connect(const std::string & host_name, int port, const std::string & user_name, const std::string & password, const std::string & db_name);
    void connect();
    
    std::unique_ptr<SQLStatement> prepare(std::string_view query) override;
    bool ping() override;
    void begin() override;
    void commit() override;
    void rollback() override;

    size_t execute(std::string_view query) override;

    bool isConnected() const { return conn != 0; }

  private:
    MYSQL * conn = 0;
    std::string host_name, user_name, password, db_name;
    int port = 0;
  };
};

#endif
