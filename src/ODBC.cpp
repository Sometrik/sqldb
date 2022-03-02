#include "ODBC.h"

#include <iostream>
#include <sqlext.h>

using namespace std;
using namespace sqldb;

ODBCStatement::ODBCStatement(stmt)
  : stmt(_stmt) {
  
}

ODBCStatement::~ODBCStatement() {
  SQLFreeHandle(SQL_HANDLE_STMT, stmt);
}

void
ODBCStatement::clearBoundData() {
  while (!bound_data.empty()) {
    query_data & d = bound_data.back();
    delete[] d.ptr;
    bound_data.pop_back();
  }
}

unsigned int
ODBCStatement::getAffectedRows() {
  if (!stmt) return false;
  SQLLEN count;
  if (SQL_SUCCEEDED(SQLRowCount(stmt, &count))) {
    if (count >= 0) {
      return (unsigned int)count;
    } else {
      fprintf(stderr, "SQLRowCount returned %d\n", count);
    }
  } else {
    error_string = CreateErrorString(SQL_HANDLE_STMT, stmt);
    fprintf(stderr, "SQLRowCount failed: %s\n", error_string.c_str()); 
  }
  return 0;
}

ODBC::ODBC(const string & _dsn, const string & _username, const string & _password)
  : dsn(_dsn),
    username(_username),
    password(_password)
{
  nts = SQL_NTS;
}

ODBC::~ODBC() {
  disconnect();
}

bool
ODBC::disconnect() {
  clearBoundData();
  if (dbc) {
    SQLDisconnect(dbc);
    SQLFreeHandle(SQL_HANDLE_DBC, dbc);
  }
  if (env) {
    SQLFreeHandle(SQL_HANDLE_ENV, env);
  }
  return true;
}

bool
ODBC::connect() {
  // retcode = SQLAllocEnv(&henv);
  if (!SQL_SUCCEEDED(SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &env))) {
    error_string = "Failed to create ODBC environment";
    cerr << "ODBC connect failed: " << error_string << endl;
    return false;
  }

  if (!SQL_SUCCEEDED(SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION, (void *) SQL_OV_ODBC3, 0))) {
    error_string = "Failed to set ODBC version";
    cerr << "ODBC connect failed: " << error_string << endl;
    return false;
  }

#if 0
  if (!SQL_SUCCEEDED(SQLSetEnvAttr(env, SQL_ATTR_AUTOCOMMIT, (void *) SQL_AUTOCOMMIT_ON, 0))) {
    error_string = "Failed to set auto commit";
    fprintf(stderr, "ODBC connect failed: %s\n", error_string.c_str());
    return false;
  }
#endif
  
  if (!SQL_SUCCEEDED(SQLAllocHandle(SQL_HANDLE_DBC, env, &dbc))) {
    error_string = "Failed to create ODBC connection handle";
    cerr << "ODBC connect failed: " << error_string << endl;
    return false;
  }
  
  string conn_str = "DSN=" + dsn + ";UID=" + username + ";PWD=" + password;

  SQLCHAR tmp[256];
  strncpy((char*)tmp, conn_str.c_str(), 256);
  tmp[255] = 0;

  cerr << "trying to connect to ODBC database, dsn = \"" << tmp << "\"\n";
  // if (SQLConnect(dbc, dsn.c_str(), dsn.size(), "hr", SQL_NTS, "hr", SQL_NTS) != SQL_SUCCESS) {
  if (!SQL_SUCCEEDED(SQLDriverConnect(dbc, 0, tmp, SQL_NTS, 0, 0, 0, SQL_DRIVER_NOPROMPT))) {
    // printSQLError(2, hdbc);
    error_string = "Failed to connect to ODBC database";
    cerr << "ODBC connect failed: " << error_string << endl;
    return false;
  }
  
  return true;
}

std::unique_ptr<ODBCStatement>
ODBC::prepare(const string & query) {
  clearBoundData();

  HSTMT stmt;
  
  if (!SQL_SUCCEEDED(SQLAllocHandle(SQL_HANDLE_STMT, dbc, &stmt))) {
    error_string = "Failed to create statement handle";
    cerr << "ODBC connect failed: " << error_string << endl;
    return false;
  }
  
  query_data & qd = bindData(query.c_str(), query.size());
  if (!SQL_SUCCEEDED(SQLPrepare(stmt, qd.ptr, qd.size))) {
    fprintf(stderr, "ODBC prepare failed\n");
    return false;
  }
  return true;
}

bool
ODBC::Finalize() {
#if 0
  if (!SQL_SUCCEEDED(SQLCloseCursor(stmt))) {
    fprintf(stderr, "ODBC close cursor failed\n");
    return false;
  }
#endif
  ClearBoundData();
  return true;
}

ODBC::query_data &
ODBC::bindData(const void * ptr, size_t size) {
  query_data d;
  d.size = (SQLINTEGER)size;
  d.ptr = new unsigned char[size];
  memcpy(d.ptr, ptr, size);
  bound_data.push_back(d);
  return bound_data.back();
}

bool
ODBC::bind(unsigned int index, unsigned int value) {
  if (!stmt) return false;
  query_data & qd = BindData(&value, sizeof(value));
  if (!SQL_SUCCEEDED(SQLBindParameter(stmt, index, SQL_PARAM_INPUT, SQL_C_ULONG,
				      SQL_INTEGER, 0, 0, qd.ptr, qd.size, &qd.size))) {
    fprintf(stderr, "ODBC failed to bind\n");
    return false;
  }
  return true;
}

bool
ODBC::bind(unsigned int index, int value) {
  if (!stmt) return false;
  query_data & qd = BindData(&value, sizeof(value));
  if (!SQL_SUCCEEDED(SQLBindParameter(stmt, index, SQL_PARAM_INPUT, SQL_C_SLONG,
				      SQL_INTEGER, 0, 0, qd.ptr, qd.size, &qd.size))) {
    fprintf(stderr, "ODBC failed to bind\n");
    return false;
  }
  return true;
}

bool
ODBC::bind(unsigned int index, double value) {
  if (!stmt) return false;
  query_data & qd = BindData(&value, sizeof(value));
  if (!SQL_SUCCEEDED(SQLBindParameter(stmt, index, SQL_PARAM_INPUT, SQL_C_DOUBLE, SQL_DOUBLE, 0, 0, qd.ptr, qd.size, &qd.size))) {
    fprintf(stderr, "ODBC failed to bind\n");
    return false;
  }
  return true;
}

bool
ODBC::bind(unsigned int index, const string & str) {
  return Bind(index, str.c_str());
}

bool
ODBC::bind(unsigned int index, const char * str) {
  if (!stmt) return false;
  query_data & qd = BindData(str, strlen(str));
  if (!SQL_SUCCEEDED(SQLBindParameter(stmt, index, SQL_PARAM_INPUT, SQL_C_CHAR,
				      SQL_CHAR, 0, 0, qd.ptr, qd.size, &qd.size))) {
    fprintf(stderr, "ODBC failed to bind\n");
    return false;
  }
  return true;
}

bool
ODBC::execute() {
  if (!stmt) return false;
  long rv = SQLExecute(stmt);
  ClearBoundData();
  if (SQL_SUCCEEDED(rv) || rv == SQL_NO_DATA) {
    return true;
  } else {
    error_string = createErrorString(SQL_HANDLE_STMT, stmt);
    fprintf(stderr, "ODBC execute failed(%d): %s\n", rv, error_string.c_str());
    return false;
  }
}

void
ODBC::Commit() {
  if (!env) return;
  SQLEndTran(SQL_HANDLE_ENV, env, SQL_COMMIT);
}

void
ODBC::Rollback() {
  if (!env) return;
  SQLEndTran(SQL_HANDLE_ENV, env, SQL_ROLLBACK);
}

string
ODBC::createErrorString(SQLSMALLINT handle_type, SQLHANDLE handle) {
  if (!handle) return "Invalid handle";

  SQLSMALLINT errRecNo = 1;
  SQLCHAR errSQLStateBuf[256];
  SQLINTEGER errCode = 0;
  SQLCHAR errMsgBuf[256]; 
  SQLSMALLINT errMsgBufLen = 256;
  SQLSMALLINT errInfoSize;
  
  memset (errSQLStateBuf, 0x00, sizeof (errSQLStateBuf));
  memset (errMsgBuf, 0x00, sizeof (errMsgBuf));

  long retcode = SQLGetDiagRec (handle_type, handle, errRecNo,
               errSQLStateBuf, &errCode, errMsgBuf, errMsgBufLen,
               &errInfoSize);

  if (retcode != SQL_NO_DATA_FOUND) {
    fprintf(stderr, "ODBC error: %s\n", (char*)errMsgBuf);
    errRecNo++;
    return (char*)errMsgBuf;
  } else {
    fprintf(stderr, "ODBC error: not found\n");
    return "";
  }
}
