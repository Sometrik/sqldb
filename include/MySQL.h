#ifndef _SQLDB_MYSQL_H_
#define _SQLDB_MYSQL_H_

#include "Connection.h"
#include "SQLStatement.h"

#include <mysql.h>
#include <vector>

#define MYSQL_MAX_BOUND_VARIABLES 255
#define MYSQL_BIND_BUFFER_SIZE 0x10000
// #define MYSQL_BIND_BUFFER_SIZE 64

namespace sqldb {
  class MySQL : public Connection {
  public:
    MySQL() { }
    ~MySQL();
    
    bool connect(const std::string & host_name, int port, const std::string & user_name, const std::string & password, const std::string & db_name);    
    bool connect();
    
    std::shared_ptr<SQLStatement> prepare(const std::string & query) override;
    bool ping() override;
    void begin() override;
    void commit() override;
    void rollback() override;

    size_t execute(const std::string & query) override;

    bool isConnected() const { return conn != 0; }

  private:
    MYSQL * conn = 0;
    std::string host_name, user_name, password, db_name;
    int port = 0;
  };

  class MySQLStatement : public SQLStatement {
  public:
    MySQLStatement(MYSQL_STMT * _stmt, const std::string & _query);
    ~MySQLStatement();
    
    size_t execute() override;
    void reset() override;
    bool next() override;

    MySQLStatement & bind(int value, bool is_defined = true) override;
    MySQLStatement & bind(long long value, bool is_defined = true) override;
    MySQLStatement & bind(const ustring & value, bool is_defined = true) override;
    MySQLStatement & bind(const char * value, bool is_defined = true) override;
    MySQLStatement & bind(const std::string & value, bool is_defined = true) override;
    MySQLStatement & bind(const void * data, size_t len, bool is_defined = true) override;
    MySQLStatement & bind(double value, bool is_defined = true) override;
  
    int getInt(int column_index, int default_value = 0) override;
    double getDouble(int column_index, double default_value = 0.0) override;
    long long getLongLong(int column_index, long long default_value = 0LL) override;
    std::string getText(int column_index, const std::string default_value = "") override;
    ustring getBlob(int column_index) override;

    bool isNull(int column_index) override;

    long long getLastInsertId() const override { return last_insert_id; }
    size_t getAffectedRows() const override { return rows_affected; }
    size_t getNumFields() override { return num_bound_variables; }
    
  protected:
    MySQLStatement & bindNull();
    MySQLStatement & bindData(enum_field_types buffer_type, const void * ptr, size_t size, bool is_defined = true, bool is_unsigned = false);
    
  private:
    MYSQL_STMT * stmt;
    unsigned int num_bound_variables = 0;       
    bool has_result_set = false, is_query_executed = false;
    long long last_insert_id = 0;
    my_bool is_null = 1, is_not_null = 0;
    unsigned int rows_affected = 0;

    MYSQL_BIND bind_data[MYSQL_MAX_BOUND_VARIABLES];
    unsigned long bind_length[MYSQL_MAX_BOUND_VARIABLES];
    my_bool bind_is_null[MYSQL_MAX_BOUND_VARIABLES];
    my_bool bind_error[MYSQL_MAX_BOUND_VARIABLES];
    char bind_buffer[MYSQL_MAX_BOUND_VARIABLES * MYSQL_BIND_BUFFER_SIZE];
    char * bind_ptr[MYSQL_MAX_BOUND_VARIABLES];
  };
};

#endif
