#ifndef _SQLDB_DATASTREAM_H_
#define _SQLDB_DATASTREAM_H_

#include "ustring.h"

#include <string>
#include <memory>

namespace sqldb {
  class DataStream {
  public:
    DataStream() { }
    virtual ~DataStream() { }

    virtual ustring getBlob(int column_index) = 0;
    virtual std::string getText(int column_index, std::string default_value) = 0;

    virtual bool isNull(int column_index) const = 0;
    virtual int getNumFields() const = 0;
    virtual int getNumRows() const = 0;
    virtual bool next() = 0;
    virtual bool seek(int row) { return false; }
    virtual bool hasExactSize() const { return false; }
    
    virtual std::string getColumnName(int column_index) const { return ""; }
    
    virtual bool getBool(int column_index, bool default_value) {
      return getInt(column_index, default_value ? 1 : 0) ? true : false;
    }
    virtual double getDouble(int column_index, double default_value = 0.0) {
      auto s = getText(column_index);
      if (!s.empty()) {
	try { return stof(s); } catch (...) { }
      }
      return default_value;
    }

    virtual int getInt(int column_index, int default_value = 0) {
      auto s = getText(column_index);
      if (!s.empty()) {
	try { return stoi(s); } catch (...) { }
      }
      return default_value;
    }

    virtual long long getLongLong(int column_index, long long default_value = 0) {
      auto s = getText(column_index);
      if (!s.empty()) {
	try { return stoll(s); } catch (...) { }
      }
      return default_value;  
    }

    virtual std::unique_ptr<DataStream> copy() const {
      // throw exception
      return std::unique_ptr<DataStream>(nullptr);
    }

    virtual void append(DataStream & other) {
      // throw exception
    }
			
    std::string getText(int column_index) { return getText(column_index, ""); }
  };
};

#endif
