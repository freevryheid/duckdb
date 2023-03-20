module test_fortran_api
  ! https://github.com/duckdb/duckdb/blob/master/test/api/capi/test_capi.cpp
  use, intrinsic :: iso_c_binding
  use duckdb
  use testdrive, only: new_unittest, unittest_type, error_type, check, skip_test
  implicit none

  private

  public :: collect_fortran_api
contains

  subroutine collect_fortran_api(testsuite)
    type(unittest_type), allocatable, intent(out) :: testsuite(:)

    testsuite = [ &
                new_unittest("basic-api-test", test_basic), &
                new_unittest("basic-null-test", test_scalar_null), &
                new_unittest("basic-string-test", test_scalar_null), &
                new_unittest("basic-bool-test", test_boolean), &
                new_unittest("basic-insert-test", test_multiple_insert), &
                new_unittest("basic-errors-test", test_error_conditions), &
                new_unittest("parquet-api-test", test_parquet) & !, &
                ! new_unittest("data-chunk-test", test_data_chunk)
                ]
  end subroutine collect_fortran_api

  subroutine test_basic(error)
    type(error_type), allocatable, intent(out) :: error

    type(c_ptr) :: db, conn
    type(duckdb_result), pointer :: result => null()

    ! Open data in in-memory mode
    call check(error, duckdb_open(c_null_ptr, db) == duckdbsuccess)
    if (allocated(error)) return

    call check(error, duckdb_connect(db, conn) == duckdbsuccess)
    if (allocated(error)) return

    allocate(result)
    call check(error, &
               duckdb_query(conn, "SELECT CAST(42 AS BIGINT);", result) == duckdbsuccess)
    if (allocated(error)) return

    call check(error, duckdb_column_type(result, 0) == duckdb_type_bigint)
    if (allocated(error)) return

    ! call check(error, duckdb_column_data(result, 0) == 42)
    ! if (allocated(error)) return

    call check(error, duckdb_column_count(result) == 1)
    if (allocated(error)) return

    call check(error, duckdb_row_count(result) == 1)
    if (allocated(error)) return

    call check(error, duckdb_value_int64(result, 0, 0) == 42)
    if (allocated(error)) return

    ! Out of range
    call check(error, duckdb_value_int64(result, 1, 0) == 0)
    if (allocated(error)) return

    call check(error, duckdb_value_int64(result, 0, 1) == 0)
    if (allocated(error)) return

    call duckdb_destroy_result(result)
    deallocate(result)
    call duckdb_disconnect(conn)
    call duckdb_close(db)
  end subroutine test_basic

  subroutine test_scalar_null(error)
    type(error_type), allocatable, intent(out) :: error

    type(c_ptr) :: database, connection, chunk
    type(duckdb_result), pointer :: result

    ! Open data in in-memory mode
    call check(error, duckdb_open(c_null_ptr, database) == duckdbsuccess)
    if (allocated(error)) return

    call check(error, duckdb_connect(database, connection) == duckdbsuccess)
    if (allocated(error)) return

    allocate(result)
    ! select scalar value
    call check(error, &
               duckdb_query(connection, "SELECT NULL", result) == duckdbsuccess)
    if (allocated(error)) return

    call check(error, duckdb_column_count(result) == 1)
    if (allocated(error)) return

    call check(error, duckdb_row_count(result) == 1)
    if (allocated(error)) return

    call check(error, duckdb_value_int64(result, 0, 0) == 0)
    if (allocated(error)) return

    call check(error, duckdb_value_is_null(result, 0, 0))
    if (allocated(error)) return

    call duckdb_destroy_result(result)
    deallocate(result)
    call duckdb_disconnect(connection)
    call duckdb_close(database)
  end subroutine test_scalar_null

  subroutine test_scalar_string(error)
    type(error_type), allocatable, intent(out) :: error

    type(c_ptr) :: database, connection, chunk
    type(duckdb_result), pointer :: result

    ! Open data in in-memory mode
    call check(error, duckdb_open(c_null_ptr, database) == duckdbsuccess)
    if (allocated(error)) return

    call check(error, duckdb_connect(database, connection) == duckdbsuccess)
    if (allocated(error)) return

    allocate(result)
    ! select scalar value
    call check(error, &
               duckdb_query(connection, "SELECT 'hello'", result) == duckdbsuccess)
    if (allocated(error)) return

    call check(error, duckdb_column_count(result) == 1)
    if (allocated(error)) return

    call check(error, duckdb_row_count(result) == 1)
    if (allocated(error)) return

    ! call check(error, duckdb_value_string(result, 0, 0) == 'hello')
    ! if (allocated(error)) return

    call check(error, .not. duckdb_value_is_null(result, 0, 0))
    if (allocated(error)) return

    call duckdb_destroy_result(result)
    deallocate(result)
    call duckdb_disconnect(connection)
    call duckdb_close(database)
  end subroutine test_scalar_string

  subroutine test_boolean(error)
    type(error_type), allocatable, intent(out) :: error

    type(c_ptr) :: database, connection, chunk
    type(duckdb_result), pointer :: result

    ! Open data in in-memory mode
    call check(error, duckdb_open(c_null_ptr, database) == duckdbsuccess)
    if (allocated(error)) return

    call check(error, duckdb_connect(database, connection) == duckdbsuccess)
    if (allocated(error)) return

    allocate(result)
    ! select true
    call check(error, &
               duckdb_query(connection, "SELECT 1=1", result) == duckdbsuccess)
    if (allocated(error)) return

    call check(error, duckdb_column_count(result) == 1)
    if (allocated(error)) return

    call check(error, duckdb_row_count(result) == 1)
    if (allocated(error)) return

    call check(error, duckdb_value_boolean(result, 0, 0))
    if (allocated(error)) return

    call check(error, .not. duckdb_value_is_null(result, 0, 0))
    if (allocated(error)) return

    ! select false
    call check(error, &
               duckdb_query(connection, "SELECT 1=0", result) == duckdbsuccess)
    if (allocated(error)) return

    call check(error, duckdb_column_count(result) == 1)
    if (allocated(error)) return

    call check(error, duckdb_row_count(result) == 1)
    if (allocated(error)) return

    call check(error, .not. duckdb_value_boolean(result, 0, 0))
    if (allocated(error)) return

    call check(error, .not. duckdb_value_is_null(result, 0, 0))
    if (allocated(error)) return

    ! select [true, false]
    call check(error, &
               duckdb_query(connection, &
               "SELECT i FROM (values (true), (false)) tbl(i) group by i order by i", &
               result) == duckdbsuccess)
    if (allocated(error)) return

    call check(error, duckdb_column_count(result) == 1)
    if (allocated(error)) return

    call check(error, duckdb_row_count(result) == 2)
    if (allocated(error)) return

    call check(error, .not. duckdb_value_boolean(result, 0, 0))
    if (allocated(error)) return

    call check(error, duckdb_value_boolean(result, 0, 1))
    if (allocated(error)) return

    call check(error, .not. duckdb_value_is_null(result, 0, 0))
    if (allocated(error)) return

    call duckdb_destroy_result(result)
    deallocate(result)
    call duckdb_disconnect(connection)
    call duckdb_close(database)
  end subroutine test_boolean

  subroutine test_multiple_insert(error)
    type(error_type), allocatable, intent(out) :: error

    type(c_ptr) :: database, connection, chunk
    type(duckdb_result), pointer :: result

    ! Open data in in-memory mode
    call check(error, duckdb_open(c_null_ptr, database) == duckdbsuccess)
    if (allocated(error)) return

    call check(error, duckdb_connect(database, connection) == duckdbsuccess)
    if (allocated(error)) return

    allocate(result)
    call check(error, &
               duckdb_query(connection, "CREATE TABLE test (a INTEGER, b INTEGER);",&
               result) == duckdbsuccess, "Table creation error.")
    if (allocated(error)) return

    call check(error, &
      duckdb_query(connection, "INSERT INTO test VALUES (11, 22)",&
      result) == duckdbsuccess, "First insert error.")
    if (allocated(error)) return

    call check(error, &
      duckdb_query(connection, "INSERT INTO test VALUES (NULL, 21)",&
      result) == duckdbsuccess, "Second insert error.")
    if (allocated(error)) return

    call check(error, &
      duckdb_query(connection, "INSERT INTO test VALUES (13, 22)",&
      result) == duckdbsuccess, "Third insert error.")
    if (allocated(error)) return
    
    call check(error, duckdb_rows_changed(result) == 1)
    if (allocated(error)) return

    call check(error, &
      duckdb_query(connection, "SELECT a, b FROM test ORDER BY a",&
      result) == duckdbsuccess, "select error.")
    if (allocated(error)) return

    ! Values in the first column
    call check(error, duckdb_value_is_null(result, 0, 0), "a(0) is not null.")
    if (allocated(error)) return

    call check(error, duckdb_value_int32(result, 0, 1) == 11, "a(1) is not 11.")
    if (allocated(error)) return

    call check(error, duckdb_value_int32(result, 0, 2) == 13, "a(2) is not 13.")
    if (allocated(error)) return

    ! Values in the second column
    call check(error, duckdb_value_int32(result, 1, 0) == 21, "b(0) is not 21.")
    if (allocated(error)) return

    call check(error, duckdb_value_int32(result, 1, 1) == 22, "b(1) is not 22.")
    if (allocated(error)) return

    call check(error, duckdb_value_int32(result, 1, 2) == 22, "b(2) is not 22.")
    if (allocated(error)) return

    ! Column names
    call check(error, duckdb_column_name(result, 0) == 'a', "column name (0) is not 'a'")
    if (allocated(error)) return

    call check(error, duckdb_column_name(result, 1) == 'b', "column name (1) is not 'b'")
    if (allocated(error)) return

    call check(error, duckdb_column_name(result, 2) == 'NULL', "column name (2) is not 'NULL'")
    if (allocated(error)) return

    call check(error, &
      duckdb_query(connection, "UPDATE test SET a = 1 WHERE b=22",&
      result) == duckdbsuccess)
    if (allocated(error)) return

    call check(error, duckdb_rows_changed(result) == 2, "Number of rows affecte different from 2")
    if (allocated(error)) return

    call duckdb_destroy_result(result)
    deallocate(result)
    call duckdb_disconnect(connection)
    call duckdb_close(database)
  end subroutine test_multiple_insert

  subroutine test_error_conditions(error)
    type(error_type), allocatable, intent(out) :: error

    type(duckdb_result), pointer :: result

    result => null()

    call check(error, .not. duckdb_value_is_null(result, 0, 0))
    if (allocated(error)) return

    call check(error, duckdb_column_type(result, 0) == duckdb_type_invalid)
    if (allocated(error)) return

    call check(error, duckdb_column_count(result) == 0)
    if (allocated(error)) return

    call check(error, duckdb_row_count(result) == 0)
    if (allocated(error)) return

    call check(error, duckdb_rows_changed(result) == 0)
    if (allocated(error)) return

    call check(error, duckdb_result_error(result) == "NULL")
    if (allocated(error)) return

    call check(error, .not. c_associated(duckdb_nullmask_data(result, 0)))
    if (allocated(error)) return

    call check(error, .not. c_associated(duckdb_column_data(result, 0)))
    if (allocated(error)) return
  end subroutine test_error_conditions

  subroutine test_parquet(error)
    type(error_type), allocatable, intent(out) :: error

    type(c_ptr) :: db, con, chunk
    type(duckdb_result), pointer :: result
    integer :: i, j, nc, nr

    ! Open data in in-memory mode
    call check(error, duckdb_open(c_null_ptr, db) == duckdbsuccess)
    if (allocated(error)) return

    call check(error, duckdb_connect(db, con) == duckdbsuccess)
    if (allocated(error)) return

    allocate(result)
    ! create a table from reading a parquet file
    call check(error, duckdb_query( &
               con, &
               "CREATE TABLE test AS SELECT * FROM 'test.parquet';", &
               result) == duckdbsuccess)
    if (allocated(error)) return

    ! create a table from reading a parquet file
    call check(error, duckdb_query( &
               con, &
               "SELECT * FROM 'test.parquet';", &
               result) == duckdbsuccess)
    if (allocated(error)) return

    print*, duckdb_column_count(result)
    ! nc = duckdb_column_count(result)
    ! call check(error, nc == 2)
    ! if (allocated(error)) return

    ! nr = duckdb_row_count(result)
    ! call check(error, nr == 5)
    ! if (allocated(error)) return

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
               "COPY (SELECT * FROM test) TO 'result.parquet' (FORMAT 'parquet');", &
               result) == duckdbsuccess)
    if (allocated(error)) return

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
    deallocate(result)
    call duckdb_disconnect(con)
    call duckdb_close(db)
  end subroutine test_parquet

  subroutine test_data_chunk(error)
    type(error_type), allocatable, intent(out) :: error
    type(c_ptr) :: db, con, chunk
    type(duckdb_result), pointer :: result
    integer :: col_count, col_idx
    integer :: chunk_count, chunk_idx
    integer :: row_count, row_idx

    ! Open db in in-memory mode
    call check(error, duckdb_open(c_null_ptr, db) == duckdbsuccess)
    if (allocated(error)) return

    call check(error, duckdb_connect(db, con) == duckdbsuccess)
    if (allocated(error)) return

    ! Create a table with 40 columns
    call check(error, duckdb_query(con, &
    "CREATE TABLE foo (c00 varchar, c01 varchar, c02 varchar, c03 varchar, c04 varchar, c05 &
    &varchar, c06 varchar, c07 varchar, c08 varchar, c09 varchar, c10 varchar, c11 varchar, c12 &
    &varchar, c13 varchar, c14 varchar, c15 varchar, c16 varchar, c17 varchar, c18 varchar, c19 &
    &varchar, c20 varchar, c21 varchar, c22 varchar, c23 varchar, c24 varchar, c25 varchar, c26 &
    &varchar, c27 varchar, c28 varchar, c29 varchar, c30 varchar, c31 varchar, c32 varchar, c33 &
    &varchar, c34 varchar, c35 varchar, c36 varchar, c37 varchar, c38 varchar, c39 varchar);", &
    result) /= duckdberror)
    if (allocated(error)) return

    ! Get table info for the created table
    call check(error, duckdb_query(con, "PRAGMA table_info(foo);", result) /= duckdberror)
    if (allocated(error)) return

    ! Columns ({cid, name, type, notnull, dflt_value, pk}}
    col_count = duckdb_column_count(result)
    call check(error, col_count == 6)
    chunk_count = duckdb_result_chunk_count(result)

    ! ! Loop over the produced chunks
    ! do chunk_idx = 0, chunk_count - 1
    !   chunk = duckdb_result_get_chunk(result, chunk_idx)
    !   row_count = duckdb_data_chunk_get_size(chunk)
    !   do row_idx = 0, row_count - 1
    !     do col_idx = 0, col_count - 1
    !       ! Get the column
    !       duckdb_vector vector = duckdb_data_chunk_get_vector(chunk, col_idx);
    !       uint64_t *validity = duckdb_vector_get_validity(vector);
    !       bool is_valid = duckdb_validity_row_is_valid(validity, row_idx);

    !       if (col_idx == 4) then
    !         ! 'dflt_value' column
    !         call check(error, is_valid == .false.)
    !         if (allocated(error)) return
    !       endif
    !     end do
    !   end do
    !   call duckdb_destroy_data_chunk(chunk)
    ! end do

    call duckdb_destroy_result(result)
    call duckdb_disconnect(con)
    call duckdb_close(db)
  end subroutine test_data_chunk
end module test_fortran_api
