program main

  use,intrinsic :: iso_c_binding
  use duckdb
  implicit none

  type(c_ptr) :: db, conn
  integer(kind(duckdb_state)) :: state
  type(duckdb_result), pointer :: res => null()
  ! character(len=:), allocatable :: col_name
  ! integer(kind(duckdb_type)) :: col_type

  print *, "clib version: ", duckdb_library_version()

  if (duckdb_open(c_null_ptr, db).eq.duckdberror) error stop "cannot open database"
  if (duckdb_connect(db, conn).eq.duckdberror) error stop "cannot connect database"

  state = duckdb_query(conn, &
    "create table integers(one integer, two integer, three integer);", &
    res)
  if (state.eq.duckdberror) error stop "cannot create table"

  state = duckdb_query(conn, &
    "insert into integers values (3, 4, 0), (5, 6, 0), (7, null, 0);", &
    res)
  if (state.eq.duckdberror) error stop "cannot insert table"

  allocate(res)

  state = duckdb_query(conn, &
    "select * from integers;", &
    res)

  print*, "Number of columns: ", duckdb_column_count(res)
  print*, "Number of rows: ", duckdb_row_count(res)

  print*, "Column 1 name: ", duckdb_column_name(res, 0)
  print*, "Column 1 type: ", duckdb_column_type(res, 0)

  print*, "here"

  call duckdb_destroy_result(res)
  call duckdb_disconnect(conn)
  call duckdb_close(db)

end program main
