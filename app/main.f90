program main

  use,intrinsic :: iso_c_binding
  use duckdb
  implicit none

  type(c_ptr) :: db, conn
  integer(kind(duckdb_state)) :: state
  type(duckdb_result), pointer :: res => null()

  print *, "clib version: ", duckdb_library_version()

  if (duckdb_open(c_null_ptr, db).eq.duckdberror) error stop "cannot open database"
  if (duckdb_connect(db, conn).eq.duckdberror) error stop "cannot connect database"

  state = duckdb_query(conn, &
    "create table integers(i integer, j integer, k integer);", &
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

  print*, duckdb_column_count(res)
  print*, duckdb_row_count(res)

  ! if(c_associated(result1)) print*, "ASS"

  ! ! print *, "result: ", result1
  ! allocate(result2)
  ! call c_f_pointer(result1, result2)

  ! ! print*, result2%column_count
  ! ! print*, result2

  ! ! if (state.eq.duckdberror) error stop "cannot select table"

  print*, "here"

  ! call duckdb_destroy_result(c_loc(res))
  call duckdb_disconnect(conn)
  call duckdb_close(db)

end program main
