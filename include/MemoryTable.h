#ifndef _SQLDB_MEMORYTABLE_H_
#define _SQLDB_MEMORYTABLE_H_

#include "DataStream.h"

#include <vector>
#include <string>
#include <map>

namespace sqldb {
  class MemoryTable : public DataStream {
  public:
    MemoryTable() : it_(data_.end()) { }
    
    std::unique_ptr<DataStream> copy() const override { return std::make_unique<MemoryTable>(*this); }

    void addColumn(std::string name, sqldb::ColumnType type) override {
      header_row_.push_back(std::pair(type, std::move(name)));
    }

    void addRow(const std::string & key) {
      it_ = data_.find(key);
      if (it_ == data_.end()) {
	data_[key] = std::vector<std::string>();
	it_ = data_.find(key);
      }
    }
    void set(int column_idx, std::string value) {
      if (it_ != data_.end()) {
	auto & row = it_->second;
	while (column_idx >= static_cast<int>(row.size())) row.push_back("");
	row[column_idx] = std::move(value);
      }
    }
    void set(int column_idx, int value) { set(column_idx, std::to_string(value)); }
    void set(int column_idx, long long value) { set(column_idx, std::to_string(value)); }
    void set(int column_idx, float value) { set(column_idx, std::to_string(value)); }
    
    bool next() override {
      if (it_ != data_.end()) {
	return ++it_ != data_.end();
      } else {
	return false;
      }
    }
        
    std::string getText(int column_index, std::string default_value) override {
      if (column_index >= 0 && it_ != data_.end()) {
	auto idx = static_cast<size_t>(column_index);
	auto & row = it_->second;
	if (idx < row.size()) return row[idx];
      }
      return std::move(default_value);    
    }
    
    ustring getBlob(int column_index) override {
      return ustring();
    }
    int getNumFields() const override { return static_cast<int>(header_row_.size()); }
    int getNumRows() const { return static_cast<int>(data_.size()); }

    ColumnType getColumnType(int column_index) const override {
      auto idx = static_cast<size_t>(column_index);
      return idx < header_row_.size() ? header_row_[idx].first : ColumnType::UNDEF;
    }

    std::string getColumnName(int column_index) const {
      auto idx = static_cast<size_t>(column_index);
      return idx < header_row_.size() ? header_row_[idx].second : "";
    }
    
    bool isNull(int column_index) const override {
      if (column_index >= 0 && it_ != data_.end()) {
	auto idx = static_cast<size_t>(column_index);
	auto & row = it_->second;
	if (idx < row.size()) return row[idx].empty();
      }
      return true;
    }

    bool seekBegin() override {
      it_ = data_.begin();
      return it_ != data_.end();
    }
    
    bool seek(const std::string & key) override {
      it_ = data_.find(key);
      return it_ != data_.end();
    }

    void append(DataStream & other) override {
      // FIXME
      header_row_.clear();
      for (int i = 0; i < other.getNumFields(); i++) {
	header_row_.push_back(std::pair(other.getColumnType(i), other.getColumnName(i)));
      }

      if (other.seekBegin()) {
	do {
	  addRow(other.getRowKey());	  
	  for (int i = 0; i < other.getNumFields(); i++) {
	    set(i, other.getText(i));
	  }
	} while (other.next());
      }
    }
      
    std::string getRowKey() const {
      if (it_ != data_.end()) return it_->first;
      else return "";
    }
    
  private:
    std::map<std::string, std::vector<std::string> > data_;
    std::map<std::string, std::vector<std::string> >::iterator it_;
    std::vector<std::pair<ColumnType, std::string> > header_row_;
  };
};

#endif
