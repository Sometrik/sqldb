#ifndef _SQLDB_CSV_H_
#define _SQLDB_CSV_H_

#include "DataStream.h"

#include <fstream>
#include <vector>
#include <cstdio>

namespace sqldb {
  class CSV : public DataStream {
  public:
    CSV(std::string csv_file);
    CSV(const CSV & other);
    
    std::unique_ptr<DataStream> copy() const override { return std::make_unique<CSV>(*this); }

    bool next() override;
       
    std::string getText(int column_index, const std::string default_value = "") override;
   
    ustring getBlob(int column_index) override {
      return ustring();
    }
    int getNumFields() const override { return static_cast<int>(header_row_.size()); }
    
    std::string getColumnName(int column_index) const {
      auto idx = static_cast<size_t>(column_index);
      return idx < header_row_.size() ? header_row_[idx] : "";
    }
    
    bool isNull(int column_index) const override {
      auto idx = static_cast<size_t>(column_index);
      return idx >= current_row_.size() || current_row_[idx].empty();
    }

    bool seekBegin() override { return seek("0"); }
    bool seek(const std::string & key) override;

    std::string getRowKey() const { return next_row_idx_ >= 1 ? std::to_string(next_row_idx_ - 1) : ""; }
    
  private:
    std::string get_record();
    
    std::string csv_file_;
    FILE * in_ = 0;
    char delimiter_ = 0;
    size_t next_row_idx_ = 0;
    std::vector<std::string> header_row_;
    std::vector<std::string> current_row_;
    std::string input_buffer_;
    std::vector<size_t> row_offsets_;
    size_t total_size_ = 0;
  };
};

#endif
