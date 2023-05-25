module test_parquet_files
  use, intrinsic :: iso_c_binding
  use duckdb
  use testdrive, only: new_unittest, unittest_type, error_type, check, skip_test
  implicit none
  private
  public :: collect_parquet_files
contains

subroutine collect_parquet_files(testsuite)

  type(unittest_type), allocatable, intent(out) :: testsuite(:)

  testsuite = [                                               &
    new_unittest("parquet-api-test", test_parquet)            &
  ]

end subroutine collect_parquet_files

  subroutine test_parquet(error)

    type(error_type), allocatable, intent(out) :: error
    type(duckdb_database) :: db
    type(duckdb_connection) :: con
    type(duckdb_result) :: result = duckdb_result()
    integer :: i, j, nc, nr

    ! Open data in in-memory mode
    call check(error, duckdb_open("", db) == duckdbsuccess)
    if (allocated(error)) return

    call check(error, duckdb_connect(db, con) == duckdbsuccess)
    if (allocated(error)) return

    ! create a table from reading a parquet file
    call check(error, duckdb_query( &
      con, &
      "CREATE TABLE test AS SELECT * FROM 'data/test.parquet';", &
      result) == duckdbsuccess)
    if (allocated(error)) return

    ! create a table from reading a parquet file
    call check(error, duckdb_query( &
      con, &
      "SELECT * FROM 'data/test.parquet';", &
      result) == duckdbsuccess)
    if (allocated(error)) return

    nc = duckdb_column_count(result)
    call check(error, nc == 2)
    if (allocated(error)) return

    nr = duckdb_row_count(result)
    call check(error, nr == 5)
    if (allocated(error)) return

    call check(error, duckdb_column_type(result, 0) == duckdb_type_bigint)
    if (allocated(error)) return

    call check(error, duckdb_column_type(result, 1) == duckdb_type_varchar)
    if (allocated(error)) return

    ! do i = 1,
    ! Retrieving the table doesn't work yet.
    ! print*, duckdb_result_chunk_count(result)
    ! duckdb_result_get_chunk(result, )

    ! Write the out the same table to a different parquet file
    call check(error, duckdb_query( &
      con, &
      "COPY (SELECT * FROM test) TO 'data/result.parquet' (FORMAT 'parquet');", &
      result) == duckdbsuccess)
    if (allocated(error)) return

    ! NOTE: parquet files moved to data directory

    !! Later get the schema from the original and the resulting parquet and compare them
    !! But it doesn't work until when we can't read the result.
    ! ! get the parquet schema
    ! call check(error, duckdb_query(                                           &
    !     con,                                                                  &
    !     "SELECT * FROM parquet_schema('test.parquet');",                      &
    !     result) == duckdbsuccess)
    ! if (allocated(error)) return

    ! ! get the parquet schema
    ! call check(error, duckdb_query(                                           &
    !     con,                                                                  &
    !     "SELECT * FROM parquet_schema('result.parquet');",                      &
    !     result) == duckdbsuccess)
    ! if (allocated(error)) return

    call duckdb_destroy_result(result)
    call duckdb_disconnect(con)
    call duckdb_close(db)

  end subroutine test_parquet
end module test_parquet_files
