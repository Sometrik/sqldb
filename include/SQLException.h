#ifndef _SQLDB_SQLEXCEPTION_H_
#define _SQLDB_SQLEXCEPTION_H_

#include <exception>
#include <string>

namespace sqldb {
  class SQLException : public std::exception {
  public:
    enum ErrorType {
      PREPARE_FAILED = 1,
      EXECUTE_FAILED,
      BIND_FAILED,
      QUERY_TIMED_OUT,
      DATABASE_ERROR,
      DATABASE_MISUSE,
      SCHEMA_CHANGED,
      BAD_BIND_INDEX,
      BAD_COLUMN_INDEX,
      GET_FAILED,
      COMMIT_FAILED,
      ROLLBACK_FAILED,
      CONSTRAINT_VIOLATION
    };
  SQLException(ErrorType _type) : type(_type) { }
  SQLException(ErrorType _type, const std::string & _errormsg)
    : type(_type), errormsg(_errormsg) { }
  SQLException(ErrorType _type, const std::string & _errormsg, const std::string & _query)
    : type(_type), errormsg(_errormsg), query(_query) { }

    ErrorType getType() const { return type; }
    const std::string & getErrorMsg() const { return errormsg; }
    const std::string & getQuery() const { return query; }
    
    const char * what() const throw() {
      switch (type) {
      case PREPARE_FAILED: return "Prepare failed";
      case EXECUTE_FAILED: return "Execute failed";
      case BIND_FAILED: return "Bind failed";
      case QUERY_TIMED_OUT: return "Query timed out";
      case DATABASE_ERROR: return "Database error";
      case DATABASE_MISUSE: return "Database misuse";
      case SCHEMA_CHANGED: return "Schema changed";
      case BAD_BIND_INDEX: return "Bad bind index";
      case BAD_COLUMN_INDEX: return "Bad column index";
      case GET_FAILED: return "Get failed";
      case COMMIT_FAILED: return "Commit failed";
      case ROLLBACK_FAILED: return "Rollback failed";
      case CONSTRAINT_VIOLATION: return "Constraint violation";
      }
      return "Unknown error";
    }
        
  private:
    ErrorType type;
    std::string errormsg, query;
  };
};

#endif
