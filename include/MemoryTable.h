#ifndef _SQLDB_MEMORYTABLE_H_
#define _SQLDB_MEMORYTABLE_H_

#include <Table.h>

#include <memory>

namespace sqldb {
  class MemoryStorage;
  
  class MemoryTable : public Table {
  public:
    MemoryTable();
    MemoryTable(std::vector<ColumnType> key_type);

    std::unique_ptr<Table> copy() const override { return std::make_unique<MemoryTable>(*this); }

    void addColumn(std::string_view name, sqldb::ColumnType type, bool unique = false) override;
    
    std::unique_ptr<Cursor> insert(const Key & key) override;
    std::unique_ptr<Cursor> insert() override;
    std::unique_ptr<Cursor> increment(const Key & key) override;
    std::unique_ptr<Cursor> assign(std::vector<int> columns) override;
    void remove(const Key & key) override;
    
    std::unique_ptr<Cursor> seekBegin() override;    
    std::unique_ptr<Cursor> seek(const Key & key) override;
    
    int getNumFields() const override;
    int getNumRows() const;
    
    ColumnType getColumnType(int column_index) const override;
    const std::string & getColumnName(int column_index) const override;
    bool isColumnUnique(int column_index) const override;
    
    void append(Table & other) override;

    void clear() override;
          
  private:
    std::shared_ptr<MemoryStorage> storage_;
  };
};

#endif
