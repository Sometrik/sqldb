
#ifndef _SQLDB_TABLE_H_
#define _SQLDB_TABLE_H_

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

    virtual std::unique_ptr<Cursor> addRow(const std::string & key) = 0;
    virtual std::unique_ptr<Cursor> addRow() = 0;
    virtual std::unique_ptr<Cursor> incrementRow(const std::string & key) = 0;
    virtual void removeRow(const std::string & key) = 0;

    virtual std::unique_ptr<Table> copy() const = 0;
    virtual void addColumn(std::string name, sqldb::ColumnType type, bool unique = false) = 0;
    virtual void append(Table & other) = 0;
    virtual void clear() = 0;

    virtual int getNumFields() const = 0;
    virtual ColumnType getColumnType(int column_index) const = 0;
    virtual bool isColumnUnique(int column_index) const { return false; }
    virtual std::string getColumnName(int column_index) const = 0;
    
    virtual void begin() { }
    virtual void commit() { }
    virtual void rollback() { }
    
    int getColumnByNames(const std::unordered_set<std::string> & names) const {
      for (int i = getNumFields() - 1; i >= 0; i--) {
	if (names.count(getColumnName(i))) return i;
      }
      return -1;
    }

    int getColumnByName(const std::string & name) const {
      for (int i = getNumFields() - 1; i >= 0; i--) {
	if (getColumnName(i) == name) return i;
      }
      return -1;
    }

    int getColumnByType(ColumnType type) const {
      for (int i = 0, n = getNumFields(); i < n; i++) {
	if (getColumnType(i) == type) return i;
      }
      return -1;
    }

    std::vector<int> getColumnsByNames(const std::unordered_set<std::string> & names) const {
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
    void addVarCharColumn(std::string name, bool unique = false) { addColumn(std::move(name), ColumnType::VARCHAR, unique); }
    void addTextColumn(std::string name) { addColumn(std::move(name), ColumnType::TEXT); }
    void addDoubleColumn(std::string name) { addColumn(std::move(name), ColumnType::DOUBLE); }
    void addURLColumn(std::string name) { addColumn(std::move(name), ColumnType::URL); }
    void addForeignKeyColumn(std::string name) { addColumn(std::move(name), ColumnType::FOREIGN_KEY); }
    void addEnumColumn(std::string name) { addColumn(std::move(name), ColumnType::ENUM); }
  };
};

#endif
