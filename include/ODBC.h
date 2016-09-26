#ifndef _SQLDB_ODBC_H_
#define _SQLDB_ODBC_H_

#include "Connection.h"

#include <windows.h>
#include <sql.h>
#include <vector>

namespace sqldb {
  class ODBC : public Connection {
  public:
    ODBCInterface(const std::string & _dsn, const std::string & _username, const std::string & _password);
    ~ODBCInterface();

    bool connect();
	
    std::shared_ptr<SQLStatement> prepare(const string & query);
      
    void commit() override;
    void rollback() override;

    bool disconnect();
    
  private:
    struct query_data {
      SQLINTEGER size;
      unsigned char * ptr;
    };
  
    string createErrorString(SQLSMALLINT handle_type, SQLHANDLE handle);
        
    string dsn, username, password;
    string error_string;
    
    HENV env = 0; // environment handle
    HDBC dbc = 0; // connection handle

    SQLINTEGER nts;
  };

  class ODBCStatement : public SQLStatement {
  public:
    ODBCStatement() { }

    bool bind(double value);
    bool bind(unsigned int value);
    bool bind(int value);
    bool bind(time_t value);
    bool bind(const string & str);
    bool bind(const char * str);
    unsigned int getAffectedRows() override;

  private:
    void clearBoundData();
    query_data & bindData(const void * ptr, size_t size);
    vector<query_data> bound_data;

    HSTMT stmt = 0; // statement handle
  };
};

#endif
