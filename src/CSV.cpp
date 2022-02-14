#include "CSV.h"

#include <iostream>

using namespace sqldb;
using namespace std;

static inline vector<string> split(const string & line, char delimiter) {
  vector<string> r;
  
  if (!line.empty()) {
    size_t i0 = 0, i = 0;
    bool in_quote = false;
    string current;
    size_t n = line.size();
    while (n >= 1 && line[n - 1] == '\r') n--; // trim carriage returns
    for ( ; i < n; i++) {
      auto c = line[i];
      if (!in_quote && c == '"') in_quote = true;
      else if (in_quote) {
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

CSV::CSV(std::string csv_file) : csv_file_(move(csv_file)) {
  cerr << "opening " << csv_file_ << "\n";
  
  in_ = fopen(csv_file_.c_str(), "r");

  if (in_) {
    fseek(in_, 0, SEEK_END);
    total_size_ = ftell(in_);
    fseek(in_, 0, SEEK_SET);
    
    cerr << "getting header\n";
    
    string s = get_record();
    
    if (delimiter_ == 0) {
      size_t best_n = 0;
      for (int i = 0; i < 3; i++) {
	char d;
	if (i == 0) d = ',';
	else if (i == 1) d = ';';
	else d = '\t';
	auto tmp = split(s, d);
	auto n = tmp.size();
	if (n > best_n) {
	  best_n = n;
	  delimiter_ = d;
	}
      }
      cerr << "delimiter = " << delimiter_ << "\n";
    }
    header_row_ = split(s, delimiter_);
  } else {
    cerr << "failed to open\n";
  }
}

CSV::CSV(const CSV & other) 
  : csv_file_(other.csv_file_),
    delimiter_(other.delimiter_),
    next_row_idx_(other.next_row_idx_),
    header_row_(other.header_row_),
    current_row_(other.current_row_),
    input_buffer_(other.input_buffer_),
    row_offsets_(other.row_offsets_),
    total_size_(other.total_size_)
{
  in_ = fopen(csv_file_.c_str(), "r");
  if (in_) fseek(in_, ftell(other.in_), SEEK_SET);
}

std::string
CSV::get_record() {
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
	return rec;
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

bool
CSV::next() {
  size_t row_offset = ftell(in_) - input_buffer_.size();
  string s = get_record();
  
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

std::string
CSV::getText(int column_index, std::string default_value) {
  if (column_index >= 0 && column_index < current_row_.size()) {
    return current_row_[column_index];
  }
  return move(default_value);
}

bool
CSV::seek(int row0) {
  size_t row = static_cast<size_t>(row0);
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
