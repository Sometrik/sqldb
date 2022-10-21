#include "CSV.h"

#include <Cursor.h>

#include <utf8proc.h>

#include <fstream>
#include <vector>
#include <cstdio>
#include <charconv>

static inline std::string normalize_nfc(const std::string & input) {
  auto r0 = utf8proc_NFC(reinterpret_cast<const unsigned char *>(input.c_str()));
  std::string r;
  if (r0) {
    r = (const char *)r0;
    free(r0);
  }
  return r;
}

using namespace sqldb;
using namespace std;

static inline vector<string> split(string_view line, char delimiter) {
  vector<string> r;
  
  if (!line.empty()) {
    size_t i = 0;
    bool in_quote = false;
    string current;
    size_t n = line.size();
    for ( ; i < n; i++) {
      auto c = line[i];
      if (c == '\r') {
	// ignore carriage returns
      } else if (!in_quote && c == '"') {
	in_quote = true;
      } else if (in_quote) {
	if (c == '\\') current += line[++i];
	else if (c == '"') in_quote = false;
	else current += c;
      } else if (c == delimiter) {
	r.push_back(current);
	current.clear();
      } else {
	current += c;
      }     
    }
    r.push_back(current);
  }
  return r;
}

class sqldb::CSVFile {
public:
  CSVFile(std::string csv_file, bool has_records) : csv_file_(move(csv_file)) {
    open(has_records);
  }

  void open(bool has_records) {
    in_ = fopen(csv_file_.c_str(), "rb");
    
    if (in_) {      
      // cerr << "getting header\n";
      
      if (has_records) {
	auto s = get_record();

	// autodetect delimiter
	if (!delimiter_) {
	  size_t best_n = 0;
	  char best_delimiter = 0;
	  for (int i = 0; i < 3; i++) {
	    char d;
	    if (i == 0) d = ',';
	    else if (i == 1) d = ';';
	    else d = '\t';
	    auto tmp = split(s, d);
	    auto n = tmp.size();
	    if (n > best_n) {
	      best_n = n;
	      best_delimiter = d;
	    }
	  }
	  // if there are no delimiters in header, use no delimiter
	  if (best_n) {
	    delimiter_ = best_delimiter;
	  }	
	  // cerr << "delimiter = " << delimiter_ << "\n";
	}
	header_row_ = split(s, delimiter_);
      } else {
	header_row_.push_back("Content");
      }
    } else {
      // cerr << "failed to open\n";
    }
  }
  
  CSVFile(const CSVFile & other) :
    csv_file_(other.csv_file_),
    delimiter_(other.delimiter_),
    next_row_idx_(other.next_row_idx_),
    header_row_(other.header_row_),
    current_row_(other.current_row_),
    input_buffer_(other.input_buffer_),
    row_offsets_(other.row_offsets_)
  {
    in_ = fopen(csv_file_.c_str(), "r");
    if (in_) fseek(in_, ftell(other.in_), SEEK_SET);
  }
  
  std::string getText(int column_index, std::string default_value) const {
    if (column_index >= 0 && column_index < current_row_.size()) {
      return current_row_[column_index];
    }
    return move(default_value);
  }
  
  int getNumFields() const { return static_cast<int>(header_row_.size()); }
  std::string getColumnName(int column_index) const {
    auto idx = static_cast<size_t>(column_index);
    return idx < header_row_.size() ? header_row_[idx] : "";
  }

  size_t getNextRowIdx() const { return next_row_idx_; }
  
  bool seek(size_t row) {
    if (row + 1 == next_row_idx_) {
      return true;
    }
    if (row < row_offsets_.size()) {
      input_buffer_.clear();
      next_row_idx_ = row;
      fseek(in_, row_offsets_[next_row_idx_], SEEK_SET);    
      return next();
    }
    if (!row_offsets_.empty()) {
      input_buffer_.clear();
      next_row_idx_ = row_offsets_.size() - 1;
      fseek(in_, row_offsets_.back(), SEEK_SET);
      row -= row_offsets_.size() - 1;
    }
    while (row > 0) {
      bool r = next();
      if (!r) return false;
      row--;
    }
    return next();
  }

  bool next() {
    size_t row_offset = ftell(in_) - input_buffer_.size();
    auto s = get_record();
    
    if (s.empty()) {
      return false;
    }
    
    current_row_ = split(s, delimiter_);
    if (next_row_idx_ == row_offsets_.size()) {
      row_offsets_.push_back(row_offset);
    }
    next_row_idx_++;
    
    return true;
  }
  
private:
  std::string get_record() {
    while ( 1 ) {
      // std::cerr << "parsing " << input_buffer_.size() << "\n";
      
      bool quoted = false;
      for (size_t i = 0; i < input_buffer_.size(); i++) {
	if (!quoted && input_buffer_[i] == '"') {
	  quoted = true;
	} else if (input_buffer_[i] == '\\') {
	  i++;
	} else if (quoted && input_buffer_[i] == '"') {
	  quoted = false;
	} else if (!quoted && input_buffer_[i] == '\n') {
	  auto rec = input_buffer_.substr(0, i);
	  // std::cerr << "found record: " << rec << "\n";
	  input_buffer_ = input_buffer_.substr(i + 1);
	  return normalize_nfc(rec);
	}
      }
      
      // std::cerr << "reading more\n";
      
      if (in_) {
	char buffer[4096];
	auto r = fread(buffer, 1, 4096, in_);
	if (r > 0) {
	  input_buffer_ += std::string(buffer, r);
	  continue;
	}
      }
      return "";
    }
  }
    
  std::string csv_file_;
  FILE * in_ = 0;
  char delimiter_ = 0;
  size_t next_row_idx_ = 0;
  std::vector<std::string> header_row_;
  std::vector<std::string> current_row_;
  std::string input_buffer_;
  std::vector<size_t> row_offsets_;
};

class CSVCursor : public Cursor {
public:
  CSVCursor(const std::shared_ptr<sqldb::CSVFile> & csv, int row) : csv_(csv) {
    csv_->seek(row);
  }
  
  bool next() override { return csv_->next(); }
  
  std::string getText(int column_index, const std::string default_value = "") override {
    return csv_->getText(column_index, default_value);    
  }

  int getNumFields() const override { return csv_->getNumFields(); }

  vector<uint8_t> getBlob(int column_index) override {
    auto v = csv_->getText(column_index, "");
    std::vector<uint8_t> r;
    for (size_t i = 0; i < v.size(); i++) r.push_back(static_cast<uint8_t>(v[i]));
    return r;
  }
  
  bool isNull(int column_index) const override {
    return csv_->getText(column_index, "").empty();    
  }
  
  std::string getRowKey() const { return csv_->getNextRowIdx() >= 1 ? std::to_string(csv_->getNextRowIdx() - 1) : ""; }

  void set(int column_idx, std::string_view value, bool is_defined = true) override {
    throw std::runtime_error("CSV is read-only");
  }
  void set(int column_idx, int value, bool is_defined = true) override {
    throw std::runtime_error("CSV is read-only");
  }
  void set(int column_idx, long long value, bool is_defined = true) override {
    throw std::runtime_error("CSV is read-only");
  }
  void set(int column_idx, double value, bool is_defined = true) override {
    throw std::runtime_error("CSV is read-only");
  }
  void set(int column_idx, const void * data, size_t len, bool is_defined = true) override {
    throw std::runtime_error("CSV is read-only");
  }
  size_t execute() override {
    throw std::runtime_error("CSV is read-only");
  }

  long long getLastInsertId() const override { return 0; }

private:
  std::shared_ptr<CSVFile> csv_;
};

CSV::CSV(std::string csv_file, bool has_records) : csv_(make_shared<CSVFile>(move(csv_file), has_records)) { }
CSV::CSV(const CSV & other) : csv_(make_shared<CSVFile>(*other.csv_)) { }
CSV::CSV(CSV && other) : csv_(move(other.csv_)) { }

int
CSV::getNumFields() const {
  return csv_->getNumFields();
}

std::string
CSV::getColumnName(int column_index) const {
  return csv_->getColumnName(column_index);
}

unique_ptr<Cursor>
CSV::seek(std::string_view key) {
  int row;
  auto result = std::from_chars(key.data(), key.data() + key.size(), row);
  if (result.ec == std::errc::invalid_argument) {
    return unique_ptr<CSVCursor>(nullptr);
  } else {
    return make_unique<CSVCursor>(csv_, row);
  }
}
