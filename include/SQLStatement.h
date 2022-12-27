#ifndef _SQLDB_SQLSTATEMENT_H_
#define _SQLDB_SQLSTATEMENT_H_

#include "DataStream.h"

namespace sqldb {
  class SQLStatement : public DataStream {
  public:
    SQLStatement() { }
    SQLStatement(std::string query) : query_(std::move(query)) { }

    void reset() override {
      DataStream::reset();
      results_available_ = false;
    }

    virtual size_t getAffectedRows() const = 0;
  
    bool resultsAvailable() const { return results_available_; }
    const std::string & getQuery() const { return query_; }

  protected:    
    bool results_available_ = false;

  private:
    std::string query_;
  };
};

#endif
