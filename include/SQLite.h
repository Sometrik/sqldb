#ifndef _SQLDB_SQLITE_H_
#define _SQLDB_SQLITE_H_

#include "Connection.h"
#include "SQLStatement.h"

#include <sqlite3.h>

namespace sqldb {
  class SQLite : public Connection {
  public:
    SQLite(const std::string & _db_file, bool read_only = false);
    ~SQLite();
  
    std::shared_ptr<sqldb::SQLStatement> prepare(const std::string & query) override;
  
  private:
    bool open(bool read_only);
  
    std::string db_file;
    sqlite3 * db;  
  };

  class SQLiteStatement : public SQLStatement {
  public:
    SQLiteStatement(sqlite3 * _db, sqlite3_stmt * _stmt);
    ~SQLiteStatement();
  
    unsigned int execute() override;
    bool next() override;
    void reset() override;

    SQLiteStatement & bind(int value, bool is_defined) override;
    SQLiteStatement & bind(long long value, bool is_defined) override;
    SQLiteStatement & bind(unsigned int value, bool is_defined) override;
    SQLiteStatement & bind(double value, bool is_defined) override;
    SQLiteStatement & bind(const char * value, bool is_defined) override;
    SQLiteStatement & bind(bool value, bool is_defined) override;
    SQLiteStatement & bind(const std::string & value, bool is_defined) override;
    SQLiteStatement & bind(const ustring & value, bool is_defined) override;
    SQLiteStatement & bind(const void* data, size_t len, bool is_defined) override;
  
    int getInt(int column_index) override;
    unsigned int getUInt(int column_index) override;
    double getDouble(int column_index) override;
    long long getLongLong(int column_index) override;
    std::string getText(int column_index) override;
    ustring getBlob(int column_index) override;
    bool getBool(int column_index) override;

    unsigned int getNumFields() override;

    long long getLastInsertId() const override;
    unsigned int getAffectedRows() const override;
    
  protected:
    void step();
    
  private:
    sqlite3_stmt * stmt;
    sqlite3 * db;
  };
};

#endif
