#ifndef _SQLDB_TABLE_H_
#define _SQLDB_TABLE_H_

#include "ColumnType.h"
#include "Cursor.h"

#include <unordered_set>
#include <memory>
#include <vector>
#include <string_view>
#include <string>
#include <numeric>

namespace sqldb {
  class Cursor;
  
  class Table {
  public:
    Table() { }
    Table(std::vector<ColumnType> key_type) : key_type_(std::move(key_type)) { }
    
    virtual ~Table() { }
    
    virtual std::unique_ptr<Cursor> seekBegin() = 0;
    virtual std::unique_ptr<Cursor> seek(const Key & key) = 0;
    virtual std::unique_ptr<Cursor> seek(int row) { return std::unique_ptr<Cursor>(nullptr); }

    virtual std::unique_ptr<Cursor> insert(const Key & key) = 0;
    virtual std::unique_ptr<Cursor> insert() = 0;
    virtual std::unique_ptr<Cursor> increment(const Key & key) = 0;
    virtual std::unique_ptr<Cursor> assign(std::vector<int> columns) = 0;
    virtual void remove(const Key & key) = 0;
    
    std::unique_ptr<Cursor> assign() {
      // Select all columns
      std::vector<int> cols(getNumFields());
      std::iota(cols.begin(), cols.end(), 0);
      return assign(std::move(cols));
    }    
    
    virtual std::unique_ptr<Table> copy() const = 0;
    virtual void addColumn(std::string_view name, sqldb::ColumnType type, bool unique = false) = 0;
    virtual void append(Table & other) = 0;
    virtual void clear() = 0;

    virtual int getNumFields() const = 0;
    virtual ColumnType getColumnType(int column_index) const = 0;
    virtual bool isColumnUnique(int column_index) const { return false; }
    virtual const std::string & getColumnName(int column_index) const = 0;
    
    virtual void begin() { }
    virtual void commit() { }
    virtual void rollback() { }
    
    int getColumnByNames(std::unordered_set<std::string> names) const {
      if (getNumFields()) {
	for (int i = getNumFields() - 1; i >= 0; i--) {
	  if (names.count(getColumnName(i))) return i;
	}
      }
      return -1;
    }

    int getColumnByName(std::string_view name) const {
      if (getNumFields()) {
	for (int i = getNumFields() - 1; i >= 0; i--) {
	  if (getColumnName(i) == name) return i;
	}
      }
      return -1;
    }

    int getColumnByType(ColumnType type) const {
      for (int i = 0, n = getNumFields(); i < n; i++) {
	if (getColumnType(i) == type) return i;
      }
      return -1;
    }

    std::vector<int> getColumnsByNames(std::unordered_set<std::string> names) const {
      std::vector<int> r;
      if (getNumFields()) {
	for (int i = getNumFields() - 1; i >= 0; i--) {
	  if (names.count(getColumnName(i))) {
	    r.push_back(i);
	  }
	}
      }
      return r;
    }
    
    void addIntColumn(std::string_view name) { addColumn(std::move(name), ColumnType::INT); }
    void addInt64Column(std::string_view name) { addColumn(std::move(name), ColumnType::INT64); }
    void addCharColumn(std::string_view name) { addColumn(std::move(name), ColumnType::CHAR); }
    void addDateTimeColumn(std::string_view name) { addColumn(std::move(name), ColumnType::DATETIME); }
    void addVarCharColumn(std::string_view name, bool unique = false) { addColumn(std::move(name), ColumnType::VARCHAR, unique); }
    void addTextColumn(std::string_view name) { addColumn(std::move(name), ColumnType::TEXT); }
    void addFloatColumn(std::string_view name) { addColumn(std::move(name), ColumnType::FLOAT); }
    void addDoubleColumn(std::string_view name) { addColumn(std::move(name), ColumnType::DOUBLE); }
    void addURLColumn(std::string_view name) { addColumn(std::move(name), ColumnType::URL); }
    void addTextKeyColumn(std::string_view name) { addColumn(std::move(name), ColumnType::TEXT_KEY); }
    void addBinaryKeyColumn(std::string_view name) { addColumn(std::move(name), ColumnType::BINARY_KEY); }
    void addEnumColumn(std::string_view name) { addColumn(std::move(name), ColumnType::ENUM); }
    void addBoolColumn(std::string_view name) { addColumn(std::move(name), ColumnType::BOOL); }
    
    std::string dumpRow(const Key & key) {
      std::string r;
      if (auto cursor = seek(key)) {
	for (int i = 0; i < getNumFields(); i++) {
	  if (i) r += ";";
	  r += cursor->getText(i);
	}
      } else {
	r += "not found";
      }
      return r;
    }

    bool hasNumericKey() const {
      return key_type_.size() == 1 && is_numeric(key_type_.front());
    }
    
    const std::vector<ColumnType> getKeyType() const { return key_type_; }
    void setKeyType(std::vector<ColumnType> key_type) { key_type_ = std::move(key_type); }
    size_t getKeySize() const { return key_type_.size(); }
    
    void setSortCol(int sort_col, int sort_subcol, bool desc = false) {
      sort_col_ = sort_col;
      sort_subcol_ = sort_subcol;
      desc_sort_ = desc;
    }
    bool hasSorting() const { return sort_col_ >= 0; }
    int getSortCol() const { return sort_col_; }
    int getSortSubcol() const { return sort_subcol_; }
    bool isDescSort() const { return desc_sort_; }

  private:
    std::vector<ColumnType> key_type_;
    int sort_col_ = -1, sort_subcol_ = -1;
    bool desc_sort_ = false;
  };
};

#endif
