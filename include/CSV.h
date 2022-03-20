#ifndef _SQLDB_CSV_H_
#define _SQLDB_CSV_H_

#include "Table.h"

#include <stdexcept>

namespace sqldb {
  class CSVFile;
  
  class CSV : public Table {
  public:
    CSV(std::string csv_file);
    CSV(const CSV & other);

    bool hasNumericKey() const override { return true; }
    std::unique_ptr<Table> copy() const override { return std::make_unique<CSV>(*this); }
       
    int getNumFields() const override;
    std::string getColumnName(int column_index) const;

    ColumnType getColumnType(int column_index) const {
      return column_index >= 0 && column_index < getNumFields() ? ColumnType::TEXT : ColumnType::UNDEF;
    }

    void append(Table & other) override {
      throw std::runtime_error("CSV is read-only");
    }

    void addColumn(std::string name, sqldb::ColumnType type) override {
      throw std::runtime_error("CSV is read-only");
    }

    std::unique_ptr<Cursor> seekBegin() override { return seek("0"); }
    std::unique_ptr<Cursor> seek(const std::string & key) override;
    
  private:
    std::shared_ptr<CSVFile> csv_;    
  };
};

#endif
