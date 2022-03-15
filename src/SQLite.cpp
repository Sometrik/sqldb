#include "SQLite.h"

#include <cstring>
#include <cassert>
#include <iostream>
#include <vector>

#include "SQLException.h"

using namespace std;
using namespace sqldb;

class SQLiteStatement : public SQLStatement {
public:
  SQLiteStatement(sqlite3 * db, sqlite3_stmt * stmt);
  ~SQLiteStatement();
  
  size_t execute() override;
  bool next() override;
  void reset() override;

  SQLiteStatement & bind(int value, bool is_defined) override;
  SQLiteStatement & bind(long long value, bool is_defined) override;
  SQLiteStatement & bind(double value, bool is_defined) override;
  SQLiteStatement & bind(const char * value, bool is_defined) override;
  SQLiteStatement & bind(const std::string & value, bool is_defined) override;
  SQLiteStatement & bind(const ustring & value, bool is_defined) override;
  SQLiteStatement & bind(const void* data, size_t len, bool is_defined) override;
  
  int getInt(int column_index, int default_value = 0) override;
  double getDouble(int column_index, double default_value = 0.0) override;
  float getFloat(int column_index, float default_value = 0.0) override;
  long long getLongLong(int column_index, long long default_value = 0) override;
  std::string getText(int column_index, std::string default_value) override;
  ustring getBlob(int column_index) override;
    
  bool isNull(int column_index) const override;

  int getNumFields() const override;

  long long getLastInsertId() const override;
  size_t getAffectedRows() const override;

  std::string getColumnName(int column_index) const override {
    if (column_index >= 0 && column_index < static_cast<int>(column_names_.size())) {
      return column_names_[column_index];
    } else {
      return "";
    }
  }

protected:
  void step();
    
private:
  sqlite3 * db_;
  sqlite3_stmt * stmt_;
  vector<const char *> column_names_;
};

SQLite::SQLite(const string & db_file, bool read_only) : db_file_(db_file), read_only_(read_only)
{
  open();
}

SQLite::SQLite(const SQLite & other) : db_file_(other.db_file_), read_only_(other.read_only_)
{
  open();
}

SQLite::SQLite(SQLite && other)
  : db_file_(std::move(other.db_file_)), db_(std::exchange(other.db_, nullptr)), read_only_(other.read_only_) { }

SQLite::~SQLite() {
  if (db_) {
    int r = sqlite3_close(db_);
    if (r) {
      cerr << "error while closing, r = " << r << endl;
    }
  }
}

static int latin1_order(unsigned char c) {
  if (c >= 'A' && c <= 'Z') return 1 + c - 'A';
  else if (c >= 'a' && c <= 'z') return 1 + c - 'a';
  else if (c == 0xc5 || c == 0xe5) return 27;
  else if (c == 0xc4 || c == 0xe4) return 28;
  else if (c == 0xd6 || c == 0xf6) return 29;
  else return c;
}

static int latin1_compare(void * arg, int len1, const void * ptr1, int len2, const void * ptr2) {
  const unsigned char * s1 = (const unsigned char *)ptr1;
  const unsigned char * s2 = (const unsigned char *)ptr2;
  for (int i = 0; i < len1 && i < len2; i++) {
    int o1 = latin1_order(s1[i]), o2 = latin1_order(s2[i]);
    if (o1 < o2) return -1;
    else if (o1 > o2) return +1;
  }
  if (len1 < len2) return -1;
  else if (len1 > len2) return +1;
  else return 0;
}

bool
SQLite::open() {
  int flags = 0;
  if (db_file_.empty()) {
    flags = SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE;
  } else if (read_only_) {
    flags = SQLITE_OPEN_READONLY;
  } else {
    flags = SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE;
  }
  int r = sqlite3_open_v2(db_file_.c_str(), &db_, flags, 0);
  if (r) {
    db_ = 0;
    throw SQLException(SQLException::OPEN_FAILED, sqlite3_errmsg(db_));
  }

  if (db_) {
    sqlite3_busy_timeout(db_, 1000);
    
    r = sqlite3_create_collation( db_,
				  "NOCASE", // "latin1",
				  SQLITE_UTF8,
				  0, // this,
				  latin1_compare
				  );
    if (r) {
      cerr << "failed to create NOCASE collation\n";
    }
  }

  return true;
}

std::unique_ptr<sqldb::SQLStatement>
SQLite::prepare(const string & query) {
  if (!db_) {
    throw SQLException(SQLException::PREPARE_FAILED);
  }
  sqlite3_stmt * stmt = 0;
  int r = sqlite3_prepare_v2(db_, query.c_str(), -1, &stmt, 0);
  if (r != SQLITE_OK) {
    throw SQLException(SQLException::PREPARE_FAILED, sqlite3_errmsg(db_));
  }
  assert(stmt);  
  return std::make_unique<SQLiteStatement>(db_, stmt);
}

SQLiteStatement::SQLiteStatement(sqlite3 * db, sqlite3_stmt * stmt) : db_(db), stmt_(stmt) {
  assert(db_);
  assert(stmt_);

  for (int i = 0, n = getNumFields(); i < n; i++) {
    column_names_.push_back(sqlite3_column_name(stmt_, i));
  }
}

SQLiteStatement::~SQLiteStatement() {
  if (stmt_) sqlite3_finalize(stmt_);
}

size_t
SQLiteStatement::execute() {
  step();
  return getAffectedRows();
}

void
SQLiteStatement::step() {
  results_available = false;

  while ( 1 ) {
    int r = sqlite3_step(stmt_);
    switch (r) {
    case SQLITE_ROW:
      results_available = true;
      return;
      
    case SQLITE_DONE:
      return;

    case SQLITE_BUSY:
      cerr << "database is busy\n";
      break;
      
    case SQLITE_ERROR: throw SQLException(SQLException::DATABASE_ERROR, sqlite3_errmsg(db_));
    case SQLITE_MISUSE: throw SQLException(SQLException::DATABASE_MISUSE, sqlite3_errmsg(db_));
    case SQLITE_CONSTRAINT: throw SQLException(SQLException::CONSTRAINT_VIOLATION, sqlite3_errmsg(db_));
      
    default:
      cerr << "unknown error: " << r << endl;
      assert(0);
      return;
    }
  }
  
  throw SQLException(SQLException::QUERY_TIMED_OUT);
}

void
SQLiteStatement::reset() {
  SQLStatement::reset();
  
  int r = sqlite3_reset(stmt_);

  switch (r) {
  case SQLITE_OK: return;
  
  case SQLITE_SCHEMA:
    throw SQLException(SQLException::SCHEMA_CHANGED, sqlite3_errmsg(db_));
    return;
    
  default:
    cerr << "unknown error: " << r << endl;
    assert(0);
    return;
  }
}

bool
SQLiteStatement::next() {
  step();
  return results_available;
}

SQLiteStatement &
SQLiteStatement::bind(int value, bool is_defined) {
  assert(stmt_);
  unsigned int index = getNextBindIndex();
  if (is_defined) {
    int r = sqlite3_bind_int(stmt_, index, value);
    if (r != SQLITE_OK) {
      throw SQLException(SQLException::BIND_FAILED, sqlite3_errmsg(db_));
    }
  }
  return *this;
}

SQLiteStatement &
SQLiteStatement::bind(long long value, bool is_defined) {
  assert(stmt_);
  unsigned int index = getNextBindIndex();
  if (is_defined) {
    int r = sqlite3_bind_int64(stmt_, index, (sqlite_int64)value);
    if (r != SQLITE_OK) {
      throw SQLException(SQLException::BIND_FAILED, sqlite3_errmsg(db_));
    }
  }
  return *this;
}

SQLiteStatement &
SQLiteStatement::bind(double value, bool is_defined) {
  assert(stmt_);
  unsigned int index = getNextBindIndex();
  if (is_defined) {
    int r = sqlite3_bind_double(stmt_, index, value);
    if (r != SQLITE_OK) {
      throw SQLException(SQLException::BIND_FAILED, sqlite3_errmsg(db_));
    }
  }
  return *this;
}

SQLiteStatement &
SQLiteStatement::bind(const char * value, bool is_defined) {
  assert(stmt_);
  unsigned int index = getNextBindIndex();
  if (is_defined) {
    assert(value);
    int r = sqlite3_bind_text(stmt_, index, value, (int)strlen(value), SQLITE_TRANSIENT);  
    if (r != SQLITE_OK) {
      throw SQLException(SQLException::BIND_FAILED, sqlite3_errmsg(db_));
    }
  }
  return *this;
}
    
SQLiteStatement &
SQLiteStatement::bind(const std::string & value, bool is_defined) {
  assert(stmt_);
  unsigned int index = getNextBindIndex();
  if (is_defined) {
    int r = sqlite3_bind_text(stmt_, index, value.c_str(), (int)value.size(), SQLITE_TRANSIENT);  
    if (r != SQLITE_OK) {
      throw SQLException(SQLException::BIND_FAILED, sqlite3_errmsg(db_));
    }
  }
  return *this;
}

SQLiteStatement &
SQLiteStatement::bind(const ustring & value, bool is_defined) {
  assert(stmt_);
  unsigned int index = getNextBindIndex();
  if (is_defined) {
    int r = sqlite3_bind_blob(stmt_, index, value.data(), value.size(), SQLITE_TRANSIENT);  
    if (r != SQLITE_OK) {
      throw SQLException(SQLException::BIND_FAILED, sqlite3_errmsg(db_));
    }
  }
  return *this;
}

SQLiteStatement &
SQLiteStatement::bind(const void* data, size_t len, bool is_defined = true) {
  assert(stmt_);
  unsigned int index = getNextBindIndex();
  if (is_defined) {
    int r = sqlite3_bind_blob(stmt_, index, data, len, SQLITE_TRANSIENT);  
    if (r != SQLITE_OK) {
      throw SQLException(SQLException::BIND_FAILED, sqlite3_errmsg(db_));
    }
  }
  return *this;
}

int
SQLiteStatement::getInt(int column_index, int default_value) {
  if (!isNull(column_index)) {
    return sqlite3_column_int(stmt_, column_index);
  }
  return default_value;
}

double
SQLiteStatement::getDouble(int column_index, double default_value) {
  if (!isNull(column_index)) {
    return sqlite3_column_double(stmt_, column_index);
  }
  return default_value;
}

float
SQLiteStatement::getFloat(int column_index, float default_value) {
  if (!isNull(column_index)) {
    return static_cast<float>(sqlite3_column_double(stmt_, column_index));
  }
  return default_value;
}

long long
SQLiteStatement::getLongLong(int column_index, long long default_value) {
  if (!isNull(column_index)) {
    return sqlite3_column_int64(stmt_, column_index);
  }
  return default_value;
}

std::string
SQLiteStatement::getText(int column_index, std::string default_value) {
  if (!isNull(column_index)) {
    const char * s = (const char *)sqlite3_column_text(stmt_, column_index);
    if (s) {
      return string(s);
    }
  }
  return move(default_value);
}

ustring
SQLiteStatement::getBlob(int column_index) {
  if (!isNull(column_index)) {
    auto data = sqlite3_column_blob(stmt_, column_index);
    auto len = sqlite3_column_bytes(stmt_, column_index);
    return ustring((const unsigned char *)data, len);
  } else {
    return ustring();
  }
}

bool
SQLiteStatement::isNull(int column_index) const {
  if (!results_available) {
    return true;
  } else {
    return stmt_ ? sqlite3_column_type(stmt_, column_index) == SQLITE_NULL : true;
  }
}

int
SQLiteStatement::getNumFields() const {
  return stmt_ ? sqlite3_column_count(stmt_) : 0;
}

long long
SQLiteStatement::getLastInsertId() const {
  return sqlite3_last_insert_rowid(db_);
}

size_t
SQLiteStatement::getAffectedRows() const {
  return sqlite3_changes(db_);
}
