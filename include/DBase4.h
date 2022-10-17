#ifndef _SQLDB_DBASE4_H_
#define _SQLDB_DBASE4_H_

#include "Table.h"
#include "Cursor.h"

#include <stdexcept>
#include <memory>

namespace sqldb {
  class DBase4File;
  
  class DBase4 : public Table {
  public:
    DBase4(std::string filename);
    DBase4(const DBase4 & other);
    DBase4(DBase4 && other);

    bool hasNumericKey() const override { return true; }
    std::unique_ptr<Table> copy() const override { return std::make_unique<DBase4>(*this); }
       
    int getNumFields() const override;
    std::string getColumnName(int column_index) const;
    ColumnType getColumnType(int column_index) const;
    
    void append(Table & other) override {
      throw std::runtime_error("dBase4 is read-only");
    }

    void clear() override {
      throw std::runtime_error("dBase4 is read-only");
    }

    void addColumn(std::string_view name, sqldb::ColumnType type, bool unique = false) override {
      throw std::runtime_error("dBase4 is read-only");
    }

    std::unique_ptr<Cursor> addRow(std::string_view key) override {
      throw std::runtime_error("dBase4 is read-only");
    }

    std::unique_ptr<Cursor> addRow() override {
      throw std::runtime_error("dBase4 is read-only");
    }

    std::unique_ptr<Cursor> incrementRow(std::string_view key) override {
      throw std::runtime_error("dBase4 is read-only");
    }

    void removeRow(std::string_view key) override {
      throw std::runtime_error("dBase4 is read-only");
    }

    std::unique_ptr<Cursor> seekBegin() override { return seek("0"); }
    std::unique_ptr<Cursor> seek(std::string_view key) override;
    
  private:    
    std::shared_ptr<DBase4File> dbf_;
  };
};

#endif

