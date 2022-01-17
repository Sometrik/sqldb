#ifndef _SQLDB_SQLSTATEMENT_H_
#define _SQLDB_SQLSTATEMENT_H_

#include "ustring.h"
#include <string>

namespace sqldb {
  class SQLStatement {
  public:
    SQLStatement() { }
  SQLStatement(const std::string & _query) : query(_query) { }

    virtual ~SQLStatement() { }

    virtual size_t execute() = 0;
    virtual bool next() = 0;
    virtual void reset() {
      next_bind_index = 1;
    }

    virtual SQLStatement & bind(bool value, bool is_defined = true) = 0;
    virtual SQLStatement & bind(const std::string & value, bool is_defined = true) = 0;
    virtual SQLStatement & bind(double value, bool is_defined = true) = 0;
    virtual SQLStatement & bind(const ustring & value, bool is_defined = true) = 0;
    virtual SQLStatement & bind(int value, bool is_defined = true) = 0;
    virtual SQLStatement & bind(const char * value, bool is_defined = true) = 0;
    virtual SQLStatement & bind(unsigned int value, bool is_defined = true) = 0;
    virtual SQLStatement & bind(const void * data, size_t len, bool is_defined = true) = 0;
    virtual SQLStatement & bind(long long value, bool is_defined = true) = 0;

    virtual double getDouble(int column_index, double default_value = 0.0) = 0;
    virtual long long getLongLong(int column_index, long long default_value = 0LL) = 0;
    virtual ustring getBlob(int column_index) = 0;
    virtual int getInt(int column_index, int default_value = 0) = 0;
    virtual bool getBool(int column_index, bool default_value = false) = 0;
    virtual std::string getText(int column_index, const std::string default_value = "") = 0;
    virtual unsigned int getUInt(int column_index, unsigned int default_value = 0) = 0;

    virtual bool isNull(int column_index) = 0;

    virtual long long getLastInsertId() const = 0;
    virtual size_t getAffectedRows() const = 0;
    virtual size_t getNumFields() = 0;
  
    bool resultsAvailable() { return results_available; }
    const std::string & getQuery() const { return query; }

  protected:
    unsigned int getNextBindIndex() { return next_bind_index++; }
    
    bool results_available = false;

  private:
    std::string query;
    unsigned int next_bind_index = 1;
  };
};

#endif
