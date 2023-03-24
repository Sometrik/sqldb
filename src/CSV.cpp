#include "CSV.h"

#include <Cursor.h>

#include <vector>
#include <cassert>
#include <charconv>

#include "utils.h"

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
    in_ = fopen(csv_file_.c_str(), "rb");
    if (in_) fseek(in_, ftell(other.in_), SEEK_SET);
  }
  
  std::string_view getText(int column_index) const {
    auto idx = static_cast<size_t>(column_index);
    return idx < current_row_.size() ? current_row_[idx] : null_string;
  }

  bool isNull(int column_index) const {
    auto idx = static_cast<size_t>(column_index);
    return idx < current_row_.size() ? current_row_[idx].empty() : true;
  }

  int getNumFields() const { return static_cast<int>(header_row_.size()); }
  
  const std::string & getColumnName(int column_index) const {
    auto idx = static_cast<size_t>(column_index);
    return idx < header_row_.size() ? header_row_[idx] : null_string;
  }

  int getNextRowIdx() const { return next_row_idx_; }
  
  bool seek(int row) {
    if (row + 1 == next_row_idx_) {
      return true;
    }
    if (row < static_cast<int>(row_offsets_.size())) {
      input_buffer_.clear();
      next_row_idx_ = row;
      fseek(in_, row_offsets_[next_row_idx_], SEEK_SET);    
      return next();
    }
    if (!row_offsets_.empty()) {
      input_buffer_.clear();
      next_row_idx_ = static_cast<int>(row_offsets_.size()) - 1;
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
    if (next_row_idx_ == static_cast<int>(row_offsets_.size())) {
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
	  auto record0 = string_view(input_buffer_).substr(0, i);
	  if (record0.back() == '\r') record0.remove_suffix(1);
	  auto r = normalize_nfc(record0);
	  // std::cerr << "found record: " << rec << "\n";
	  input_buffer_ = input_buffer_.substr(i + 1);
	  return r;
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

      // If the last row is not \n terminated, return it anyway
      if (!input_buffer_.empty()) {
	auto r = normalize_nfc(input_buffer_);
	input_buffer_.clear();
	return r;
      } else {
	return "";
      }
    }
  }
    
  std::string csv_file_;
  FILE * in_ = 0;
  char delimiter_ = 0;
  int next_row_idx_ = 0;
  std::vector<std::string> header_row_;
  std::vector<std::string> current_row_;
  std::string input_buffer_;
  std::vector<size_t> row_offsets_;
  
  static inline std::string null_string;
};

class CSVCursor : public Cursor {
public:
  CSVCursor(const std::shared_ptr<sqldb::CSVFile> & csv, int row, int sheet) : csv_(csv), sheet_(sheet) {
    csv_->seek(row);
    updateRowKey();
  }
  
  bool next() override {
    if (csv_->next()) {
      updateRowKey();
      return true;
    } else {
      return false;
    }
  }
  
  std::string_view getText(int column_index) override {
    return csv_->getText(column_index);
  }

  double getDouble(int column_index, double default_value = 0.0) override {
    auto s = getText(column_index);
    if (!s.empty()) {
      double d;
      auto [ ptr, ec ] = std::from_chars(s.data(), s.data() + s.size(), d);
      if (ec == std::errc()) return d;
    }
    return default_value;
  }

  float getFloat(int column_index, float default_value = 0.0f) override {
    auto s = getText(column_index);
    if (!s.empty()) {
      float f;
      auto [ ptr, ec ] = std::from_chars(s.data(), s.data() + s.size(), f);
      if (ec == std::errc()) return f;	
    }
    return default_value;
  }

  int getInt(int column_index, int default_value = 0) override {
    auto s = getText(column_index);
    if (!s.empty()) {
      int i;
      auto [ ptr, ec ] = std::from_chars(s.data(), s.data() + s.size(), i);
      if (ec == std::errc()) return i;	
    }
    return default_value;
  }

  long long getLongLong(int column_index, long long default_value = 0) override {
    auto s = getText(column_index);
    if (!s.empty()) {
      long long ll;
      auto [ ptr, ec ] = std::from_chars(s.data(), s.data() + s.size(), ll);
      if (ec == std::errc()) return ll;	
    }
    return default_value;  
  }

  Key getKey(int column_index) override {
    return Key(getText(column_index));
  }

  int getNumFields() const override { return csv_->getNumFields(); }

  vector<uint8_t> getBlob(int column_index) override {
    auto v = csv_->getText(column_index);
    std::vector<uint8_t> r;
    for (size_t i = 0; i < v.size(); i++) r.push_back(static_cast<uint8_t>(v[i]));
    return r;
  }
  
  bool isNull(int column_index) const override {
    return csv_->isNull(column_index);
  }

  const std::string & getColumnName(int column_index) override {
    return csv_->getColumnName(column_index);
  }
  
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
  size_t update(const Key & key) override {
    throw std::runtime_error("CSV is read-only");
  }

  long long getLastInsertId() const override { return 0; }

protected:
  void updateRowKey() {
    Key key;
    key.addComponent(sheet_);
    if (csv_->getNextRowIdx() >= 1) key.addComponent(csv_->getNextRowIdx() - 1);
    setRowKey(std::move(key));
  }

private:
  int sheet_;
  std::shared_ptr<CSVFile> csv_;
};

CSV::CSV(std::string csv_file, bool has_records) {
  csv_.push_back(make_shared<CSVFile>(move(csv_file), has_records));
  
  std::vector<ColumnType> key_type = { ColumnType::INT, ColumnType::INT };
  setKeyType(std::move(key_type));
}

CSV::CSV(const CSV & other): Table(other) {
  for (auto & csv : other.csv_) {
    csv_.push_back(make_shared<CSVFile>(*csv));
  }
}

CSV::CSV(CSV && other)
  : Table(other),
    csv_(move(other.csv_)) { }

int
CSV::getNumFields(int sheet) const {
  if (sheet >= 0 && sheet < static_cast<int>(csv_.size())) {
    return csv_[sheet]->getNumFields();
  } else {
    return 0;
  }
}

const std::string &
CSV::getColumnName(int column_index, int sheet) const {
  if (sheet >= 0 && sheet < static_cast<int>(csv_.size())) {
    return csv_[sheet]->getColumnName(column_index);
  } else {
    return empty_string;
  }
}

unique_ptr<Cursor>
CSV::seek(const Key & key) {
  return seek(key.getInt(1), key.getInt(0));
}

unique_ptr<Cursor>
CSV::seek(int row, int sheet) {
  return make_unique<CSVCursor>(csv_[sheet], row, sheet);
}
