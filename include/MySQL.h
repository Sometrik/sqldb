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
    MySQL(bool _use_utf8 = true);
    ~MySQL();
    
    bool connect(const std::string & host_name, int port, const std::string & user_name, const std::string & password, const std::string & db_name);    
    bool connect();
    
    std::shared_ptr<SQLStatement> prepare(const std::string & query) override;
    bool ping() override;
    void begin() override;
    void commit() override;
    void rollback() override;

    unsigned int execute(const char * query) override;

  private:
    MYSQL * conn = 0;
    std::string host_name, user_name, password, db_name;
    int port = 0;
    bool use_utf8;
  };

  class MySQLStatement : public SQLStatement {
  public:
    MySQLStatement(MYSQL_STMT * _stmt, const std::string & _query);
    ~MySQLStatement();
    
    unsigned int execute() override;
    void reset() override;
    bool next() override;

    void bind(int value, bool is_defined = true) override;
    void bind(long long value, bool is_defined = true) override;
    void bind(const ustring & value, bool is_defined = true) override;
    void bind(const char * value, bool is_defined = true) override;
    void bind(bool value, bool is_defined = true) override;
    void bind(unsigned int value, bool is_defined = true) override;
    void bind(const std::string & value, bool is_defined = true) override;
    void bind(const void * data, size_t len, bool is_defined = true) override;
    void bind(double value, bool is_defined = true) override;
  
    int getInt(int column_index) override;
    unsigned int getUInt(int column_index) override;
    double getDouble(int column_index) override;
    long long getLongLong(int column_index) override;
    std::string getText(int column_index) override;
    ustring getBlob(int column_index) override;
    
    long long getLastInsertId() const { return last_insert_id; }
    unsigned int getAffectedRows() const { return rows_affected; }
    unsigned int getNumFields() { return num_bound_variables; }
    
  protected:
    void bindNull(int index);
    void bindData(int index, enum_field_types buffer_type, const void * ptr, unsigned int size, bool is_defined = true, bool is_unsigned = false);
    
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
