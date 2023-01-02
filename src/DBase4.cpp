#include "DBase4.h"

#include <Cursor.h>

#include <utf8proc.h>

#include <vector>
#include <cassert>
#include <shapefil.h>

#include "utils.h"

using namespace std;
using namespace sqldb;

class sqldb::DBase4File {
public:
  DBase4File(std::string fn, int primary_key) : fn_(std::move(fn)), primary_key_(primary_key) {
    h_ = DBFOpen(fn_.c_str(), "rb");
    if (h_) {
      initialize();
    } else {
      // cerr << "failed to open DBF " << fn_ << endl;
    }
  }

  DBase4File(const DBase4File & other) : fn_(other.fn_), primary_key_(other.primary_key_) {
    h_ = DBFOpen(fn_.c_str(), "rb");
    if (h_) {
      initialize();
    } else {
      // cerr << "failed to open DBF " << fn_ << endl;
    }
  }

  ~DBase4File() {
    if (h_) {
      DBFClose(h_);
    }
  }

  std::string getText(int row_index, int column_index) const {
    if (!isNull(row_index, column_index)) {
      auto r = DBFReadStringAttribute(h_, row_index, column_index);
      if (r) return normalize_nfc(r);
    }
    return "";
  }

  bool getBool(int row_index, int column_index, bool default_value) const {
    if (!isNull(row_index, column_index)) {
      return DBFReadLogicalAttribute(h_, row_index, column_index);
    } else {
      return default_value;
    }
  }
  
  int getInt(int row_index, int column_index, int default_value) const {
    if (!isNull(row_index, column_index)) {
      return DBFReadIntegerAttribute(h_, row_index, column_index);
    } else {
      return default_value;
    }
  }
  
  double getDouble(int row_index, int column_index, double default_value) const {
    if (!isNull(row_index, column_index)) {
      return DBFReadDoubleAttribute(h_, row_index, column_index);
    } else {
      return default_value;
    }
  }
  
  bool isNull(int row_index, int column_index) const {
    return row_index < 0 || row_index >= record_count_ || DBFIsAttributeNULL(h_, row_index, column_index);
  }

  Key getRowKey(int row) {
    Key key;
    if (row >= 0) {
      if (primary_key_ == -1) key.addComponent(row);
      else key.addComponent(getText(row, primary_key_));
    }
    return key;
  }
  
  int getRecordCount() { return record_count_; }
  int getNumFields() const { return static_cast<int>(column_types_.size()); }
  
  const std::string & getColumnName(int column_index) const {
    auto idx = static_cast<size_t>(column_index);
    return idx < column_names_.size() ? column_names_[idx] : null_string;
  }

  ColumnType getColumnType(int column_index) const {
    auto idx = static_cast<size_t>(column_index);
    return idx < column_types_.size() ? column_types_[idx] : ColumnType::UNDEF;
  }
  
private:
  void initialize() {
    record_count_ = DBFGetRecordCount(h_);
  
    auto n = DBFGetFieldCount(h_);
    for (int i = 0; i < n; i++) {
      char name[255];
      auto type = DBFGetFieldInfo(h_, i, name, 0, 0);
      column_names_.push_back(name);
      
      switch (type) {
      case FTString:
	column_types_.push_back(ColumnType::VARCHAR);
	break;
      case FTInteger:
	column_types_.push_back(ColumnType::INT);
	break;
      case FTDouble:
	column_types_.push_back(ColumnType::DOUBLE);
	break;
      case FTLogical:
	column_types_.push_back(ColumnType::BOOL);
	break;
      case FTInvalid:
      default:
	column_types_.push_back(ColumnType::UNDEF);
	break;
      }
    }
  }

  std::string fn_;
  int primary_key_;
  
  DBFHandle h_;
  std::vector<std::string> column_names_;
  std::vector<ColumnType> column_types_;
  int record_count_;

  static inline std::string null_string;
};

class DBase4Cursor : public Cursor {
public:
  DBase4Cursor(std::shared_ptr<sqldb::DBase4File> dbf, int row) : dbf_(std::move(dbf)), current_row_(row) {
  }
  
  bool next() override {
    if (current_row_ + 1 < dbf_->getRecordCount()) {
      current_row_++;
      text_cache_.clear();
      return true;
    } else {
      return false;
    }
  }
  
  std::string_view getText(int column_index) override {
    auto [ it, is_new ] = text_cache_.emplace(column_index, std::string());
    if (is_new) it->second = dbf_->getText(current_row_, column_index);
    return it->second;
  }

  bool getBool(int column_index, bool default_value = false) override {
    return dbf_->getBool(current_row_, column_index, default_value);
  }

  double getDouble(int column_index, double default_value = 0.0) override {
    return dbf_->getDouble(current_row_, column_index, default_value);    
  }
  
  int getInt(int column_index, int default_value = 0) override {
    return dbf_->getInt(current_row_, column_index, default_value);
  }

  long long getLongLong(int column_index, long long default_value = 0) override {
    return dbf_->getInt(current_row_, column_index, default_value);
  }

  int getNumFields() const override { return dbf_->getNumFields(); }

  vector<uint8_t> getBlob(int column_index) override {
    auto v = dbf_->getText(current_row_, column_index);
    std::vector<uint8_t> r;
    for (size_t i = 0; i < v.size(); i++) r.push_back(static_cast<uint8_t>(v[i]));
    return r;
  }
  
  bool isNull(int column_index) const override {
    return dbf_->isNull(current_row_, column_index);
  }

  const std::string & getColumnName(int column_index) override {
    return dbf_->getColumnName(column_index);
  }

  Key getRowKey() const override {
    return dbf_->getRowKey(current_row_);
  }

  void set(int column_idx, std::string_view value, bool is_defined = true) override {
    throw std::runtime_error("dBase4 file is read-only");
  }
  void set(int column_idx, int value, bool is_defined = true) override {
    throw std::runtime_error("dBase4 file is read-only");
  }
  void set(int column_idx, long long value, bool is_defined = true) override {
    throw std::runtime_error("dBase4 file is read-only");
  }
  void set(int column_idx, double value, bool is_defined = true) override {
    throw std::runtime_error("dBase4 file is read-only");
  }
  void set(int column_idx, const void * data, size_t len, bool is_defined = true) override {
    throw std::runtime_error("dBase4 file is read-only");
  }
  size_t execute() override {
    throw std::runtime_error("dBase4 file is read-only");
  }
  size_t update(const Key & key) override {
    throw std::runtime_error("dBase4 file is read-only");
  }

  long long getLastInsertId() const override { return 0; }

private:
  std::shared_ptr<DBase4File> dbf_;
  int current_row_;
  std::unordered_map<int, std::string> text_cache_;
};

DBase4::DBase4(std::string filename, int primary_key)
  : dbf_(make_shared<DBase4File>(move(filename), primary_key)) { }

DBase4::DBase4(const DBase4 & other)
  : Table(other),
    dbf_(make_shared<DBase4File>(*other.dbf_)),
    primary_key_mapping_(other.primary_key_mapping_) { }

DBase4::DBase4(DBase4 && other)
  : Table(other),
    dbf_(move(other.dbf_)),
    primary_key_mapping_(move(other.primary_key_mapping_)) { }

int
DBase4::getNumFields() const {
  return dbf_->getNumFields();
}

const std::string &
DBase4::getColumnName(int column_index) const {
  return dbf_->getColumnName(column_index);
}

ColumnType
DBase4::getColumnType(int column_index) const {
  return dbf_->getColumnType(column_index);
}

unique_ptr<Cursor>
DBase4::seek(const Key & key) {
  assert(key.size() == 1);
  if (!primary_key_mapping_.empty()) {
    auto it = primary_key_mapping_.find(key.getText(0));
    if (it != primary_key_mapping_.end()) {
      return seek(it->second);
    } else {
      return unique_ptr<DBase4Cursor>(nullptr);
    }
  } else {
    return seek(key.getInt(0));
  }
}

unique_ptr<Cursor>
DBase4::seek(int row) {
  return make_unique<DBase4Cursor>(dbf_, row);
}
