#ifndef _TABLE_H_
#define _TABLE_H_

#include "ColumnType.h"

#include <unordered_set>
#include <memory>
#include <vector>
#include <string>

namespace sqldb {
  class Cursor;
  
  class Table {
  public:
    Table() { }
    virtual ~Table() { }

    virtual bool hasNumericKey() const { return false; }
    virtual std::unique_ptr<Cursor> seekBegin() = 0;
    virtual std::unique_ptr<Cursor> seek(const std::string & key) = 0;

    virtual std::unique_ptr<Table> copy() const = 0;
    virtual void addColumn(std::string name, sqldb::ColumnType type) = 0;
    virtual void append(Table & other) = 0;

    virtual int getNumFields() const = 0;
    virtual ColumnType getColumnType(int column_index) const = 0;
    virtual std::string getColumnName(int column_index) const = 0;
    
    int getColumnByName(const std::unordered_set<std::string> & names) const {
      for (int i = getNumFields() - 1; i >= 0; i--) {
	if (names.count(getColumnName(i))) {
	  return i;
	}
      }
      return -1;
    }
    
    std::vector<int> getColumnsByName(const std::unordered_set<std::string> & names) const {
      std::vector<int> r;
      for (int i = getNumFields() - 1; i >= 0; i--) {
	if (names.count(getColumnName(i))) {
	  r.push_back(i);
	}
      }
      return r;
    }
    
    void addIntColumn(std::string name) { addColumn(std::move(name), ColumnType::INT); }
    void addInt64Column(std::string name) { addColumn(std::move(name), ColumnType::INT64); }
    void addCharColumn(std::string name) { addColumn(std::move(name), ColumnType::CHAR); }
    void addDateTimeColumn(std::string name) { addColumn(std::move(name), ColumnType::DATETIME); }
    void addTextColumn(std::string name) { addColumn(std::move(name), ColumnType::TEXT); }
    void addDoubleColumn(std::string name) { addColumn(std::move(name), ColumnType::DOUBLE); }
    void addURLColumn(std::string name) { addColumn(std::move(name), ColumnType::URL); }
    void addForeignKeyColumn(std::string name) { addColumn(std::move(name), ColumnType::FOREIGN_KEY); }
  };
};

#endif
