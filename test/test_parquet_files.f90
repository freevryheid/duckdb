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
    type(duckdb_data_chunk) :: chunk
    type(duckdb_vector) :: vector
    type(duckdb_logical_type) :: type
    integer :: i, j, nc, nr, row_count, col_count

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


    do i = 0, duckdb_result_chunk_count(result)-1
      chunk = duckdb_result_get_chunk(result, i)
      col_count = duckdb_data_chunk_get_column_count(chunk)
      row_count = duckdb_data_chunk_get_size(chunk)
      print*,  col_count, row_count
      do j = 0, col_count - 1
        vector = duckdb_data_chunk_get_vector(chunk, j)
        type = duckdb_vector_get_column_type(vector)
        print*, i, j, duckdb_get_type_id(type)
      enddo
    enddo

    ! Write the out the same table to a different parquet file
    call check(error, duckdb_query( &
      con, &
      "COPY (SELECT * FROM test) TO 'data/result.parquet' (FORMAT 'parquet');", &
      result) == duckdbsuccess)
    if (allocated(error)) return

    !! Later get the schema from the original and the resulting parquet and compare them
    !! But it doesn't work until when we can't read the result.
    ! get the parquet schema
    call check(error, duckdb_query(                                           &
        con,                                                                  &
        "SELECT * FROM parquet_schema('data/test.parquet');",                      &
        result) == duckdbsuccess)
    if (allocated(error)) return

    ! get the parquet schema
    call check(error, duckdb_query(                                           &
        con,                                                                  &
        "SELECT * FROM parquet_schema('data/result.parquet');",                      &
        result) == duckdbsuccess)
    if (allocated(error)) return

    call duckdb_destroy_result(result)
    call duckdb_disconnect(con)
    call duckdb_close(db)

  end subroutine test_parquet
end module test_parquet_files
