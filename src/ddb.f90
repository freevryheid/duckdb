module ddb
  use :: duckdb, &
  & duckdb_database   => dbd, &
  & duckdb_connection => dbc, &
  & duckdb_open       => db_open, &
  & duckdb_connect    => db_connect, &
  & duckdb_disconnect => db_disconnect, &
  & duckdb_close      => db_close
end module ddb
