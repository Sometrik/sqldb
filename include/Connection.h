#ifndef _SQLDB_CONNECTION_H_
#define _SQLDB_CONNECTION_H_

#include <SQLStatement.h>

#include <string>
#include <memory>

namespace sqldb {
  class SQLStatement;
  
  class Connection {
  public:
    Connection() { }
    Connection(const Connection & other) = delete;
    virtual ~Connection() { }
    Connection & operator=(const Connection & other) = delete;
    
    virtual std::unique_ptr<sqldb::SQLStatement> prepare(const std::string & query) = 0;
    virtual void begin() {
      execute("BEGIN TRANSACTION");
    }
    virtual void commit() {
      execute("COMMIT");
    }
    virtual void rollback() {
      execute("ROLLBACK");
    }
    virtual size_t execute(const std::string & query) {
      auto stmt = prepare(query);
      return stmt->execute();
    }
    virtual bool ping() { return true; }    
    virtual bool isConnected() const = 0;

    std::string quote(const std::string & str, bool is_defined = true) const {
      if (!is_defined) return "NULL";
	
      std::string output = "\"";
      for (unsigned int i = 0; i < str.size(); i++) {
	switch (str[i]) {
	case '\\':
	  output += "\\\\";
	  break;
	case '\'':
	  output += "\\'";
	  break;
	case '"':
	  output += "\\\"";
	  break;
	case 0:
	  output += "\\0";
	  break;
	case 0x0a:
	  output += "\\n";
	  break;
	case 0x0d:
	  output += "\\r";
	  break;
	case 0x08:
	  output += "\\b";
	  break;
	case 0x1a:
	  output += "\\Z";
	  break;
	default:
	  output += str[i];
	}
      }
      output += "\"";
      return output;
    }

    std::string quote(const long long v, bool is_defined = true) const {
      if (!is_defined) return "NULL";
      return std::to_string(v);      
    }

    std::string quote(const int v, bool is_defined = true) const {
      if (!is_defined) return "NULL";
      return std::to_string(v);      
    }
    
    std::string quote(const unsigned short v, bool is_defined = true) const {
      if (!is_defined) return "NULL";
      return std::to_string(v);      
    }

    std::string quote(const float v, bool is_defined = true) const {
      if (!is_defined) return "NULL";
      return std::to_string(v);      
    }

    std::string quote(const double v, bool is_defined = true) const {
      if (!is_defined) return "NULL";
      return std::to_string(v);      
    }

  };
};

#endif
