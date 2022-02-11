#ifndef _SQLDB_SQLSTATEMENT_H_
#define _SQLDB_SQLSTATEMENT_H_

#include "DataStream.h"

namespace sqldb {
  class SQLStatement : public DataStream {
  public:
    SQLStatement() { }
    SQLStatement(const std::string & _query) : query(_query) { }

    virtual size_t execute() = 0;
    virtual void reset() {
      next_bind_index = 1;
    }

    virtual SQLStatement & bind(const std::string & value, bool is_defined = true) = 0;
    virtual SQLStatement & bind(double value, bool is_defined = true) = 0;
    virtual SQLStatement & bind(const ustring & value, bool is_defined = true) = 0;
    virtual SQLStatement & bind(int value, bool is_defined = true) = 0;
    virtual SQLStatement & bind(const char * value, bool is_defined = true) = 0;
    virtual SQLStatement & bind(const void * data, size_t len, bool is_defined = true) = 0;
    virtual SQLStatement & bind(long long value, bool is_defined = true) = 0;

    virtual SQLStatement & bind(bool value, bool is_defined) {
      return bind(value ? 1 : 0, is_defined);
    }

    virtual long long getLastInsertId() const = 0;
    virtual size_t getAffectedRows() const = 0;
  
    bool resultsAvailable() const { return results_available; }
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
