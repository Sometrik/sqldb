#ifndef _SQLDB_DATASTREAM_H_
#define _SQLDB_DATASTREAM_H_

#include "ColumnType.h"

#include <string>
#include <vector>

namespace sqldb {  
  class DataStream {
  public:
    DataStream() { }
    DataStream(const DataStream & other) = delete;
    DataStream(const DataStream && other) = delete;

    DataStream & operator=(const DataStream & other) = delete;
    DataStream & operator=(const DataStream && other) = delete;
    
    virtual ~DataStream() { }

    virtual size_t execute() = 0;

    virtual std::vector<uint8_t> getBlob(int column_index) = 0;
    virtual std::string getText(int column_index, std::string default_value) = 0;

    virtual bool isNull(int column_index) const = 0;
    virtual int getNumFields() const = 0;
    virtual bool next() = 0;

    virtual ColumnType getColumnType(int column_index) const {
      return column_index >= 0 && column_index < getNumFields() ? ColumnType::TEXT : ColumnType::UNDEF;
    }
    virtual std::string getColumnName(int column_index) const { return "Column #" + std::to_string(column_index); }
    
    virtual bool getBool(int column_index, bool default_value = false) {
      return getInt(column_index, default_value ? 1 : 0) ? true : false;
    }

    virtual double getDouble(int column_index, double default_value = 0.0) {
      auto s = getText(column_index);
      if (!s.empty()) {
	try { return stof(s); } catch (...) { }
      }
      return default_value;
    }
    virtual float getFloat(int column_index, float default_value = 0.0f) {
      auto s = getText(column_index);
      if (!s.empty()) {
       try { return static_cast<float>(stof(s)); } catch (...) { }
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
			
    std::string getText(int column_index) { return getText(column_index, ""); }

    virtual void set(int column_idx, std::string_view value, bool is_defined = true) = 0;
    virtual void set(int column_idx, int value, bool is_defined = true) = 0;
    virtual void set(int column_idx, long long value, bool is_defined = true) = 0;
    virtual void set(int column_idx, double value, bool is_defined = true) = 0;
    virtual void set(int column_idx, const void * data, size_t len, bool is_defined) = 0;

    virtual long long getLastInsertId() const = 0;
  };
};

#endif
