#ifndef _SQLDB_SQLEXCEPTION_H_
#define _SQLDB_SQLEXCEPTION_H_

#include <exception>
#include <string>

namespace sqldb {
  class SQLException : public std::exception {
  public:
    enum ErrorType {
      INIT_FAILED = 1,
      CONNECTION_FAILED,
      PREPARE_FAILED,
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
      CONSTRAINT_VIOLATION,
      OPEN_FAILED,
      MISMATCH      
    };
  SQLException(ErrorType type) noexcept : type_(type) { }
  SQLException(ErrorType type, const std::string & errormsg) noexcept
    : type_(type), errormsg_(errormsg) { }
  SQLException(ErrorType type, const std::string & errormsg, const std::string & query) noexcept
    : type_(type), errormsg_(errormsg), query_(query) { }

    ErrorType getType() const noexcept { return type_; }
    const std::string & getErrorMsg() const noexcept { return errormsg_; }
    const std::string & getQuery() const noexcept { return query_; }
    
    const char * what() const noexcept override {
      switch (type_) {
      case INIT_FAILED: return "Init failed";
      case CONNECTION_FAILED: return "Connection failed";
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
      case OPEN_FAILED: return "Database opening failed";
      case MISMATCH: return "Datatype mismatch";
      }
      return "Unknown error";
    }
        
  private:
    ErrorType type_;
    std::string errormsg_, query_;
  };
};

#endif
