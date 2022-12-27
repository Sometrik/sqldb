#ifndef _SQLDB_CSV_H_
#define _SQLDB_CSV_H_

#include "Table.h"
#include "Cursor.h"

#include <stdexcept>

namespace sqldb {
  class CSVFile;
  
  class CSV : public Table {
  public:
    CSV(std::string csv_file, bool has_records = true);
    CSV(const CSV & other);
    CSV(CSV && other);

    std::unique_ptr<Table> copy() const override { return std::make_unique<CSV>(*this); }
       
    int getNumFields() const override;
    std::string getColumnName(int column_index) const override;

    ColumnType getColumnType(int column_index) const override {
      return column_index >= 0 && column_index < getNumFields() ? ColumnType::TEXT : ColumnType::UNDEF;
    }

    void append(Table & other) override {
      throw std::runtime_error("CSV is read-only");
    }

    void clear() override {
      throw std::runtime_error("CSV is read-only");
    }

    void addColumn(std::string_view name, sqldb::ColumnType type, bool unique = false) override {
      throw std::runtime_error("CSV is read-only");
    }

    std::unique_ptr<Cursor> insert(const Key & key) override {
      throw std::runtime_error("CSV is read-only");
    }

    std::unique_ptr<Cursor> insert() override {
      throw std::runtime_error("CSV is read-only");
    }

    std::unique_ptr<Cursor> increment(const Key & key) override {
      throw std::runtime_error("CSV is read-only");
    }

    std::unique_ptr<Cursor> assign(std::vector<int> columns) override {
      throw std::runtime_error("CSV is read-only");
    }
    
    void remove(const Key & key) override {
      throw std::runtime_error("CSV is read-only");
    }

    std::unique_ptr<Cursor> seekBegin() override { return seek(0); }
    std::unique_ptr<Cursor> seek(const Key & key) override;
    std::unique_ptr<Cursor> seek(int row);
    
  private:
    std::shared_ptr<CSVFile> csv_;    
  };
};

#endif
