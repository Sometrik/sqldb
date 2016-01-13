#include "SQLite.h"

#include <cstring>
#include <cassert>
#include <iostream>

#include "SQLException.h"

using namespace std;
using namespace sqldb;

SQLite::SQLite(const string & _db_file, bool read_only)
  : db_file(_db_file) 
{
  open(read_only);
}

SQLite::~SQLite() {
  if (db) {
    int r = sqlite3_close(db);
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
SQLite::open(bool read_only) {
  int flags = 0;
  if (db_file.empty()) {
    flags = SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE;
  } else if (read_only) {
    flags = SQLITE_OPEN_READONLY;
  } else {
    flags = SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE;
  }
  int r = sqlite3_open_v2(db_file.c_str(), &db, flags, 0);
  if (r) {
    cerr << "Can't open database " << db_file << ": " << sqlite3_errmsg(db) << endl;
    // sqlite3_close(db);
    db = 0;
  }

  if (db) {
    sqlite3_busy_timeout(db, 1000);
    
    r = sqlite3_create_collation( db,
				  "NOCASE", // "latin1",
				  SQLITE_UTF8,
				  0, // this,
				  latin1_compare
				  );
    if (r) {
      fprintf(stderr, "failed to create NOCASE collation: %s\n", sqlite3_errmsg(db));
    }
  }

  return true;
}

std::shared_ptr<sqldb::SQLStatement>
SQLite::prepare(const string & query) {
  if (!db) {
    throw SQLException(SQLException::PREPARE_FAILED);
  }
  sqlite3_stmt * stmt = 0;
  int r = sqlite3_prepare_v2(db, query.c_str(), -1, &stmt, 0);
  if (r != SQLITE_OK) {
    throw SQLException(SQLException::PREPARE_FAILED, sqlite3_errmsg(db));
  }
  assert(stmt);  
  return std::make_shared<SQLiteStatement>(db, stmt);
}

SQLiteStatement::SQLiteStatement(sqlite3 * _db, sqlite3_stmt * _stmt) : db(_db), stmt(_stmt) {
  assert(db);
  assert(stmt);
}

SQLiteStatement::~SQLiteStatement() {
  if (stmt) sqlite3_finalize(stmt);
}

unsigned int
SQLiteStatement::execute() {
  step();
  return getAffectedRows();
}

void
SQLiteStatement::step() {
  results_available = false;

  while ( 1 ) {
    int r = sqlite3_step(stmt);
    switch (r) {
    case SQLITE_ROW:
      results_available = true;
      return;
      
    case SQLITE_DONE:
      return;

    case SQLITE_BUSY:
      cerr << "database is busy\n";
      break;
      
    case SQLITE_ERROR: throw SQLException(SQLException::DATABASE_ERROR, sqlite3_errmsg(db));
    case SQLITE_MISUSE: throw SQLException(SQLException::DATABASE_MISUSE, sqlite3_errmsg(db));
    case SQLITE_CONSTRAINT: throw SQLException(SQLException::CONSTRAINT_VIOLATION, sqlite3_errmsg(db));
      
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
  
  int r = sqlite3_reset(stmt);

  switch (r) {
  case SQLITE_OK: return;
  
  case SQLITE_SCHEMA:
    throw SQLException(SQLException::SCHEMA_CHANGED, sqlite3_errmsg(db));
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
  assert(stmt);
  unsigned int index = getNextBindIndex();
  if (is_defined) {
    int r = sqlite3_bind_int(stmt, index, value);
    if (r != SQLITE_OK) {
      throw SQLException(SQLException::BIND_FAILED, sqlite3_errmsg(db));
    }
  }
  return *this;
}

SQLiteStatement &
SQLiteStatement::bind(unsigned int value, bool is_defined) {
  assert(stmt);
  unsigned int index = getNextBindIndex();
  if (is_defined) {
    int r = sqlite3_bind_int64(stmt, index, value);
    if (r != SQLITE_OK) {
      throw SQLException(SQLException::BIND_FAILED, sqlite3_errmsg(db));
    }
  }
  return *this;
}

SQLiteStatement &
SQLiteStatement::bind(long long value, bool is_defined) {
  assert(stmt);
  unsigned int index = getNextBindIndex();
  if (is_defined) {
    int r = sqlite3_bind_int64(stmt, index, (sqlite_int64)value);
    if (r != SQLITE_OK) {
      throw SQLException(SQLException::BIND_FAILED, sqlite3_errmsg(db));
    }
  }
  return *this;
}

SQLiteStatement &
SQLiteStatement::bind(double value, bool is_defined) {
  assert(stmt);
  unsigned int index = getNextBindIndex();
  if (is_defined) {
    int r = sqlite3_bind_double(stmt, index, value);
    if (r != SQLITE_OK) {
      throw SQLException(SQLException::BIND_FAILED, sqlite3_errmsg(db));
    }
  }
  return *this;
}

SQLiteStatement &
SQLiteStatement::bind(const char * value, bool is_defined) {
  assert(stmt);
  unsigned int index = getNextBindIndex();
  if (is_defined) {
    assert(value);
    int r = sqlite3_bind_text(stmt, index, value, (int)strlen(value), SQLITE_TRANSIENT);  
    if (r != SQLITE_OK) {
      throw SQLException(SQLException::BIND_FAILED, sqlite3_errmsg(db));
    }
  }
  return *this;
}

SQLiteStatement &
SQLiteStatement::bind(bool value, bool is_defined) {
  return bind(value ? 1 : 0, is_defined);
}
    
SQLiteStatement &
SQLiteStatement::bind(const std::string & value, bool is_defined) {
  assert(stmt);
  unsigned int index = getNextBindIndex();
  if (is_defined) {
    int r = sqlite3_bind_text(stmt, index, value.c_str(), (int)value.size(), SQLITE_TRANSIENT);  
    if (r != SQLITE_OK) {
      throw SQLException(SQLException::BIND_FAILED, sqlite3_errmsg(db));
    }
  }
  return *this;
}

SQLiteStatement &
SQLiteStatement::bind(const ustring & value, bool is_defined) {
  assert(stmt);
  unsigned int index = getNextBindIndex();
  if (is_defined) {
    int r = sqlite3_bind_blob(stmt, index, value.data(), value.size(), SQLITE_TRANSIENT);  
    if (r != SQLITE_OK) {
      throw SQLException(SQLException::BIND_FAILED, sqlite3_errmsg(db));
    }
  }
  return *this;
}

SQLiteStatement &
SQLiteStatement::bind(const void* data, size_t len, bool is_defined = true) {
  assert(stmt);
  unsigned int index = getNextBindIndex();
  if (is_defined) {
    int r = sqlite3_bind_blob(stmt, index, data, len, SQLITE_TRANSIENT);  
    if (r != SQLITE_OK) {
      throw SQLException(SQLException::BIND_FAILED, sqlite3_errmsg(db));
    }
  }
  return *this;
}

int
SQLiteStatement::getInt(int column_index) {
  assert(stmt);
  if (results_available) {
    return sqlite3_column_int(stmt, column_index);
  }
  return 0;
}

unsigned int
SQLiteStatement::getUInt(int column_index) {
  assert(stmt);
  if (results_available) {
    return (unsigned int)sqlite3_column_int64(stmt, column_index);
  }
  return 0;
}

double
SQLiteStatement::getDouble(int column_index) {
  assert(stmt);
  if (results_available) {
    return sqlite3_column_double(stmt, column_index);
  }
  return 0;
}

long long
SQLiteStatement::getLongLong(int column_index) {
  assert(stmt);
  if (results_available) {
    return sqlite3_column_int64(stmt, column_index);
  }
  return 0;
}

bool
SQLiteStatement::getBool(int column_index) {
  return getInt(column_index) ? true : false;
}

std::string
SQLiteStatement::getText(int column_index) {
  assert(stmt);
  if (results_available) {
    const char * s = (const char *)sqlite3_column_text(stmt, column_index);
    if (s) {
      return string(s);
    }
  }
  return string();
}

ustring
SQLiteStatement::getBlob(int column_index) {
  assert(stmt);
  assert(results_available);
  if (results_available) {
    unsigned char * data = (unsigned char *)sqlite3_column_blob(stmt, column_index);
    unsigned int len = sqlite3_column_bytes(stmt, column_index);
    return ustring(data, len);
  } else {
    return ustring();
  }
}

unsigned int
SQLiteStatement::getNumFields() {
  assert(stmt);
  return sqlite3_column_count(stmt);
}

long long
SQLiteStatement::getLastInsertId() const {
  return sqlite3_last_insert_rowid(db);
}

unsigned int
SQLiteStatement::getAffectedRows() const {
  return sqlite3_changes(db);
}
