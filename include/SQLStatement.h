#ifndef _SQLDB_SQLSTATEMENT_H_
#define _SQLDB_SQLSTATEMENT_H_

#include "DataStream.h"

namespace sqldb {
  class SQLStatement : public DataStream {
  public:
    SQLStatement() { }
    SQLStatement(std::string query) : query_(std::move(query)) { }

    virtual void reset() {
      results_available_ = false;
      next_bind_index_ = 0;
    }

    SQLStatement & bind(int value, bool is_defined = true) {
      set(getNextBindIndex(), value, is_defined);
      return *this;
    }
    SQLStatement & bind(long long value, bool is_defined = true) {
      set(getNextBindIndex(), value, is_defined);
      return *this;
    }  
    SQLStatement & bind(double value, bool is_defined = true) {
      set(getNextBindIndex(), value, is_defined);
      return *this;
    }
    SQLStatement & bind(std::string_view value, bool is_defined = true) {
      set(getNextBindIndex(), std::move(value), is_defined);
      return *this;
    }
    SQLStatement & bind(const void * data, size_t len, bool is_defined) {
      set(getNextBindIndex(), data, len, is_defined);
      return *this;
    }

    SQLStatement & bind(bool value, bool is_defined = true) {
      return bind(value ? 1 : 0, is_defined);
    }

    virtual size_t getAffectedRows() const = 0;
  
    bool resultsAvailable() const { return results_available_; }
    const std::string & getQuery() const { return query_; }

  protected:
    int getNextBindIndex() { return next_bind_index_++; }
    
    bool results_available_ = false;

  private:
    std::string query_;
    int next_bind_index_ = 0;
  };
};

#endif
