module test_parquet_files
  use, intrinsic :: iso_c_binding
  use duckdb
  use strings, only: string_t
  use constants
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
    type(duckdb_vector) :: vec
    type(c_ptr) :: data_ptr
    type(string_t), allocatable :: col1_p(:)
    integer(int32), pointer  :: col2_p(:)
    real(real32), pointer :: col3_p(:)

    type(string_t), allocatable :: col1(:)
    integer(int32), allocatable :: col2(:)
    real(real32), allocatable :: col3(:)
    integer :: chunk_count, chunk_idx, j, nc, nr
    integer :: start_index, end_index, row_count

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
    call check(error, nc == 3, 'number of columns')
    if (allocated(error)) return

    nr = duckdb_row_count(result)
    call check(error, nr == 3, 'number of rows')
    if (allocated(error)) return

    call check(error, duckdb_column_type(result, 0) == duckdb_type_varchar, 'col 1 type')
    if (allocated(error)) return

    call check(error, duckdb_column_type(result, 1) == duckdb_type_integer, 'col 2 type')
    if (allocated(error)) return

    call check(error, duckdb_column_type(result, 2) == duckdb_type_float, 'col 3 type')
    if (allocated(error)) return

    chunk_count = duckdb_result_chunk_count(result)
    call check(error, chunk_count == 2, 'wrong chunk count')
    if (allocated(error)) return

    allocate(col1(nr), col2(nr), col3(nr))
    start_index=1
    do chunk_idx = 0, chunk_count - 1
      chunk = duckdb_result_get_chunk(result, chunk_idx)
      row_count = duckdb_data_chunk_get_size(chunk)

      end_index = start_index + row_count - 1

      vec = duckdb_data_chunk_get_vector(chunk, 1)
      data_ptr = duckdb_vector_get_data(vec)
      call c_f_pointer(data_ptr, col2_p, [row_count])
      col2(start_index:end_index) = col2_p

      vec = duckdb_data_chunk_get_vector(chunk, 2)
      data_ptr = duckdb_vector_get_data(vec)
      call c_f_pointer(data_ptr, col3_p, [row_count])
      col3(start_index:end_index) = col3_p
      start_index = end_index + 1
    enddo

    print*, col2
    print*, col3
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

  !! take 3 fortran arrays of different types and put them in a duckdb in-memory table. then write to parquet.
end module test_parquet_files
