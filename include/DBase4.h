#ifndef _SQLDB_DBASE4_H_
#define _SQLDB_DBASE4_H_

#include "Table.h"
#include "Cursor.h"

#include <stdexcept>
#include <memory>
#include <unordered_map>

namespace sqldb {
  class DBase4File;
  
  class DBase4 : public Table {
  public:
    DBase4(std::string filename, int primary_key = -1);
    DBase4(const DBase4 & other);
    DBase4(DBase4 && other);

    std::unique_ptr<Table> copy() const override { return std::make_unique<DBase4>(*this); }
       
    int getNumFields() const override;
    const std::string & getColumnName(int column_index) const override;
    ColumnType getColumnType(int column_index) const override;
    
    void append(Table & other) override {
      throw std::runtime_error("dBase4 is read-only");
    }

    void clear() override {
      throw std::runtime_error("dBase4 is read-only");
    }

    void addColumn(std::string_view name, sqldb::ColumnType type, bool unique = false) override {
      throw std::runtime_error("dBase4 is read-only");
    }

    std::unique_ptr<Cursor> insert(const Key & key) override {
      throw std::runtime_error("dBase4 is read-only");
    }

    std::unique_ptr<Cursor> insert() override {
      throw std::runtime_error("dBase4 is read-only");
    }

    std::unique_ptr<Cursor> increment(const Key & key) override {
      throw std::runtime_error("dBase4 is read-only");
    }

    std::unique_ptr<Cursor> assign(std::vector<int> columns) override {
      throw std::runtime_error("dBase4 is read-only");
    }

    void remove(const Key & key) override {
      throw std::runtime_error("dBase4 is read-only");
    }

    std::unique_ptr<Cursor> seekBegin() override { return seek(0); }
    std::unique_ptr<Cursor> seek(const Key & key) override;
    std::unique_ptr<Cursor> seek(int row) override;

    void setPrimaryKeyMapping(std::unordered_map<sqldb::Key, int> m) { primary_key_mapping_ = std::move(m); }

  private:    
    std::shared_ptr<DBase4File> dbf_;
    std::unordered_map<sqldb::Key, int> primary_key_mapping_;
  };
};

#endif

