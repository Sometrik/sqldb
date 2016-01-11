#ifndef _SQLDB_SQLSTATEMENT_H_
#define _SQLDB_SQLSTATEMENT_H_

#include "SQLException.h"

#include "ustring.h"

#include <ctime>
#include <string>

namespace sqldb {
  class SQLStatement {
  public:
    SQLStatement() { }
  SQLStatement(const std::string & _query) : query(_query) { }

    virtual ~SQLStatement() { }

    virtual unsigned int execute() = 0;
    virtual bool next() = 0;
    virtual void reset() {
      next_bind_index = 1;
    }

    virtual void bind(bool value, bool is_defined = true);
    virtual void bind(const std::string & value, bool is_defined = true);
    virtual void bind(double value, bool is_defined = true);
    virtual void bind(const ustring & value, bool is_defined = true);
    virtual void bind(int value, bool is_defined = true);
    virtual void bind(const char * value, bool is_defined = true);
    virtual void bind(unsigned int value, bool is_defined = true);
    virtual void bind(const void* data, size_t len, bool is_defined = true);
    virtual void bind(long long value, bool is_defined = true);

    virtual double getDoubleValue(int column_index);
    virtual long long getLongLongValue(int column_index);
    virtual ustring getBlobValue(int column_index);
    virtual int getIntValue(int column_index);
    virtual bool getBoolValue(int column_index);
    virtual std::string getTextValue(int column_index);
    virtual unsigned int getUIntValue(int column_index);

    virtual long long getLastInsertId() const = 0;
    virtual unsigned int getRowsAffected() const = 0;
    virtual unsigned int getNumFields() = 0;
  
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
