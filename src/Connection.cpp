#include "Connection.h"

#include "SQLStatement.h"

using namespace std;
using namespace sqldb;

unsigned int
Connection::execute(const char * query) {
  auto stmt = prepare(query);
  return stmt->execute();
}

void
Connection::begin() {
  execute("BEGIN TRANSACTION");
}

void
Connection::commit() {
  execute("COMMIT");
}

void
Connection::rollback() {
  execute("ROLLBACK");
}
