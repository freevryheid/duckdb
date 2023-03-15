#include <duckdb.h>


void main() {


  duckdb_database db;
  duckdb_connection con;
  duckdb_state state;
  duckdb_result result;


  if (duckdb_open(NULL, &db) == DuckDBError) {
  	// handle error
  }
  if (duckdb_connect(db, &con) == DuckDBError) {
  	// handle error
  }

  // create a table
  state = duckdb_query(con, "CREATE TABLE integers(i INTEGER, j INTEGER);", NULL);
  if (state == DuckDBError) {
      // handle error
  }
  // insert three rows into the table
  state = duckdb_query(con, "INSERT INTO integers VALUES (3, 4), (5, 6), (7, NULL);", NULL);
  if (state == DuckDBError) {
      // handle error
  }
  // query rows again
  state = duckdb_query(con, "SELECT * FROM integers", &result);
  if (state == DuckDBError) {
      // handle error
  }
  // handle the result

  int32_t *i_data = (int32_t *) duckdb_column_data(&result, 0);
  int32_t *j_data = (int32_t *) duckdb_column_data(&result, 1);
  bool    *i_mask = duckdb_nullmask_data(&result, 0);
  bool    *j_mask = duckdb_nullmask_data(&result, 1);
  idx_t row_count = duckdb_row_count(&result);
  for(idx_t row = 0; row < row_count; row++) {
      if (i_mask[row]) {
          printf("NULL");
      } else {
          printf("%d", i_data[row]);
      }
      printf(",");
      if (j_mask[row]) {
          printf("NULL");
      } else {
          printf("%d", j_data[row]);
      }
      printf("\n");
  }

  // destroy the result after we are done with it
  duckdb_destroy_result(&result);

  // cleanup
  duckdb_disconnect(&con);
  duckdb_close(&db);

}

