#ifndef _SQLDB_MEMORYTABLE_H_
#define _SQLDB_MEMORYTABLE_H_

#include "DataStream.h"

#include <vector>
#include <string>

namespace sqldb {
  class MemoryTable : public DataStream {
  public:
    MemoryTable() { }
    
    std::unique_ptr<DataStream> copy() const override { return std::make_unique<MemoryTable>(*this); }
    
    void addColumn(std::string name) { header_row_.push_back(std::move(name)); }
    void addRow() {
      data_.push_back(std::vector<std::string>());
      current_row_idx_ = data_.size() - 1;
    }
    void set(int column_idx, std::string value) {
      if (current_row_idx_ < data_.size()) {
	auto & row = data_[current_row_idx_];
	while (column_idx >= static_cast<int>(row.size())) row.push_back("");
	row[column_idx] = std::move(value);
      }
    }
    void set(int column_idx, int value) { set(column_idx, std::to_string(value)); }
    void set(int column_idx, long long value) { set(column_idx, std::to_string(value)); }
    
    bool next() override {
      if (current_row_idx_ + 1 < data_.size()) {
	current_row_idx_++;
	return true;
      } else {
	return false;
      }
    }
        
    std::string getText(int column_index, std::string default_value) override {
      if (column_index >= 0) {
	auto idx = static_cast<size_t>(column_index);
	auto & row = data_[current_row_idx_];
	if (idx < row.size()) return row[idx];
      }
      return std::move(default_value);    
    }
    
    ustring getBlob(int column_index) override {
      return ustring();
    }
    int getNumFields() const override { return static_cast<int>(header_row_.size()); }
    int getNumRows() const override { return static_cast<int>(data_.size()); }
    
    std::string getColumnName(int column_index) const {
      auto idx = static_cast<size_t>(column_index);
      return idx < header_row_.size() ? header_row_[idx] : "";
    }
    
    bool isNull(int column_index) const override {
      if (column_index >= 0) {
	auto idx = static_cast<size_t>(column_index);
	auto & row = data_[current_row_idx_];
	if (idx < row.size()) return row[idx].empty();
      }
      return true;
    }
    
    bool seek(int row) {
      if (row >= 0 && row < data_.size()) {
	current_row_idx_ = static_cast<size_t>(row);
	return true;
      } else {
	return false;
      }
    }

    void append(DataStream & other) override {
      // FIXME
      header_row_.clear();
      for (int i = 0; i < other.getNumFields(); i++) {
	header_row_.push_back(other.getColumnName(i));
      }

      if (other.seek(0)) {
	do {
	  data_.push_back(std::vector<std::string>());
	  auto & row = data_.back();
	  
	  for (int i = 0; i < other.getNumFields(); i++) {
	    row.push_back(other.getText(i));
	  }
	} while (other.next());
      }
    }

  private:
    std::vector<std::vector<std::string> > data_;
    size_t current_row_idx_ = 0;
    std::vector<std::string> header_row_;    
  };
};

#endif
