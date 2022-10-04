#ifndef _SQLDB_MEMORYTABLE_H_
#define _SQLDB_MEMORYTABLE_H_

#include <Table.h>

#include <memory>

namespace sqldb {
  class MemoryStorage;
  
  class MemoryTable : public Table {
  public:
    MemoryTable(bool numeric_key = false);

    bool hasNumericKey() const override { return numeric_key_; }

    std::unique_ptr<Table> copy() const override { return std::make_unique<MemoryTable>(*this); }

    void addColumn(std::string_view name, sqldb::ColumnType type, bool unique = false) override;
    
    std::unique_ptr<Cursor> addRow(std::string_view key) override;
    std::unique_ptr<Cursor> addRow() override;
    std::unique_ptr<Cursor> incrementRow(std::string_view key) override;
    void removeRow(std::string_view key) override;
    
    std::unique_ptr<Cursor> seekBegin() override;    
    std::unique_ptr<Cursor> seek(std::string_view key) override;
    
    int getNumFields() const override;
    int getNumRows() const;
    
    ColumnType getColumnType(int column_index) const override;
    std::string getColumnName(int column_index) const;
    bool isColumnUnique(int column_index) const;
    
    void append(Table & other) override;

    void clear() override;
          
  private:
    bool numeric_key_;
    std::shared_ptr<MemoryStorage> storage_;
  };
};

#endif
