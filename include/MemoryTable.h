#ifndef _SQLDB_MEMORYTABLE_H_
#define _SQLDB_MEMORYTABLE_H_

#include <Table.h>

#include <vector>
#include <unordered_map>
#include <memory>

namespace sqldb {
  class Cursor;
  
  class MemoryTable : public Table {
  public:
    MemoryTable(bool numeric_key = false) : numeric_key_(numeric_key) { }

    bool hasNumericKey() const override { return numeric_key_; }

    std::unique_ptr<Table> copy() const override { return std::make_unique<MemoryTable>(*this); }

    void addColumn(std::string name, sqldb::ColumnType type, bool unique = false) override {
      header_row_.push_back(std::tuple(type, std::move(name), unique));
    }
    
    std::unique_ptr<Cursor> addRow(const std::string & key) override;
    std::unique_ptr<Cursor> addRow() override { return addRow(std::to_string(++auto_increment_)); }
            
    std::unique_ptr<Cursor> seekBegin() override;    
    std::unique_ptr<Cursor> seek(const std::string & key) override;
    
    int getNumFields() const override { return static_cast<int>(header_row_.size()); }
    int getNumRows() const { return static_cast<int>(data_.size()); }
    ColumnType getColumnType(int column_index) const override {
      auto idx = static_cast<size_t>(column_index);
      return idx < header_row_.size() ? std::get<0>(header_row_[idx]) : ColumnType::UNDEF;
    }

    std::string getColumnName(int column_index) const {
      auto idx = static_cast<size_t>(column_index);
      return idx < header_row_.size() ? std::get<1>(header_row_[idx]) : "";
    }

    bool isColumnUnique(int column_index) const {
      auto idx = static_cast<size_t>(column_index);
      return idx < header_row_.size() ? std::get<2>(header_row_[idx]) : false;
    }
    
    void append(Table & other) override;
          
  private:
    bool numeric_key_;
    std::unordered_map<std::string, std::vector<std::string> > data_;
    std::vector<std::tuple<ColumnType, std::string, bool> > header_row_;
    long long auto_increment_ = 0;
  };
};

#endif
