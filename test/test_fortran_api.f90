module test_fortran_api

  ! https://github.com/duckdb/duckdb/blob/master/test/api/capi/test_capi.cpp
  use, intrinsic :: iso_c_binding
  use constants
  use duckdb
  use testdrive, only: new_unittest, unittest_type, error_type, check, skip_test
  implicit none
  private
  public :: collect_fortran_api
  contains

    subroutine collect_fortran_api(testsuite)

      type(unittest_type), allocatable, intent(out) :: testsuite(:)

      testsuite = [                                               &
        new_unittest("basic-api-test", test_basic),               &
        new_unittest("basic-null-test", test_scalar_null),        &
        new_unittest("basic-string-test", test_scalar_string),    &
        new_unittest("basic-bool-test", test_boolean),            &
        new_unittest("basic-insert-test", test_multiple_insert),  &
        new_unittest("basic-errors-test", test_error_conditions)  &
        ]

    end subroutine collect_fortran_api

    subroutine test_basic(error)

      type(error_type), allocatable, intent(out) :: error
      type(duckdb_database) :: db
      type(duckdb_connection) :: conn
      type(duckdb_result) :: ddb_result = duckdb_result()
      integer(kind=int64), pointer :: data_out
      type(c_ptr) :: data_in

      ! print *, new_line('a') // "duckdb library " // duckdb_library_version() // new_line('a')

      ! Open data in in-memory mode
      call check(error, duckdb_open(c_null_ptr, db) == duckdbsuccess, "open database")
      if (allocated(error)) return

      call check(error, duckdb_connect(db, conn) == duckdbsuccess, "connect database")
      if (allocated(error)) return

      call check(error, &
        duckdb_query(conn, "SELECT CAST(42 AS BIGINT);", ddb_result) == duckdbsuccess, &
        "query database")
      if (allocated(error)) return

      call check(error, duckdb_column_type(ddb_result, 0) == duckdb_type_bigint, "database column type")
      if (allocated(error)) return

      ! **DEPRECATED**: Prefer using `duckdb_result_get_chunk` instead
      ! data_in = duckdb_column_data(ddb_result, 0)
      ! if (c_associated(data_in)) then
      !   allocate(data_out)
      !   call c_f_pointer(data_in, data_out)
      ! end if
      ! call check(error, data_out == 42)

      ! TODO: Uncommenting above causes the following to fail (and vice vera)
      ! maybe because chunk functions cannot be mixed with the legacy result functions
      call check(error, duckdb_result_chunk_count(ddb_result) == 1, "chunk count")
      if (allocated(error)) return

      call check(error, duckdb_column_count(ddb_result) == 1, "database column count")
      if (allocated(error)) return

      call check(error, duckdb_row_count(ddb_result) == 1, "database row count")
      if (allocated(error)) return

      call check(error, duckdb_value_int64(ddb_result, 0, 0) == 42, "col 0 row 0 value")
      if (allocated(error)) return

      ! Out of range
      call check(error, duckdb_value_int64(ddb_result, 1, 0) == 0, "col 1 row 0 value")
      if (allocated(error)) return

      call check(error, duckdb_value_int64(ddb_result, 0, 1) == 0, "col 0 row 1 value")
      if (allocated(error)) return

      call duckdb_destroy_result(ddb_result)
      call duckdb_disconnect(conn)
      call duckdb_close(db)

    end subroutine test_basic

    subroutine test_scalar_null(error)

      type(error_type), allocatable, intent(out) :: error
      type(duckdb_database) :: db
      type(duckdb_connection) :: conn
      type(duckdb_result) :: ddb_result = duckdb_result()

      ! Open data in in-memory mode
      call check(error, duckdb_open(c_null_ptr, db) == duckdbsuccess)
      if (allocated(error)) return

      call check(error, duckdb_connect(db, conn) == duckdbsuccess)
      if (allocated(error)) return

      ! select scalar value
      call check(error, duckdb_query(conn, "SELECT NULL", ddb_result) == duckdbsuccess)
      if (allocated(error)) return

      call check(error, duckdb_column_count(ddb_result) == 1)
      if (allocated(error)) return

      call check(error, duckdb_row_count(ddb_result) == 1)
      if (allocated(error)) return

      call check(error, duckdb_value_int64(ddb_result, 0, 0) == 0)
      if (allocated(error)) return

      call check(error, duckdb_value_is_null(ddb_result, 0, 0))
      if (allocated(error)) return

      call duckdb_destroy_result(ddb_result)
      call duckdb_disconnect(conn)
      call duckdb_close(db)

    end subroutine test_scalar_null

    subroutine test_scalar_string(error)

      type(error_type), allocatable, intent(out) :: error
      type(duckdb_database) :: db
      type(duckdb_connection) :: conn
      type(duckdb_result) :: ddb_result = duckdb_result()
      type(duckdb_string) :: dstr = duckdb_string()
      character(len=:), pointer :: str

      ! Open data in in-memory mode
      call check(error, duckdb_open(c_null_ptr, db) == duckdbsuccess)
      if (allocated(error)) return

      call check(error, duckdb_connect(db, conn) == duckdbsuccess)
      if (allocated(error)) return

      ! select scalar value
      call check(error, &
        duckdb_query(conn, "SELECT 'hello'", ddb_result) == duckdbsuccess)
      if (allocated(error)) return

      call check(error, duckdb_column_count(ddb_result) == 1)
      if (allocated(error)) return

      call check(error, duckdb_row_count(ddb_result) == 1)
      if (allocated(error)) return

      dstr = duckdb_value_string(ddb_result, 0, 0)
      call c_f_pointer(dstr%data, str)
      str => str(1:dstr%size)
      call check(error, str == 'hello')
      if (allocated(error)) return

      call check(error, .not. duckdb_value_is_null(ddb_result, 0, 0))
      if (allocated(error)) return

      ! dstr needs to be freed (before destroying result)
      str => null()
      dstr = duckdb_string()

      call duckdb_destroy_result(ddb_result)
      call duckdb_disconnect(conn)
      call duckdb_close(db)

    end subroutine test_scalar_string

    subroutine test_boolean(error)

      type(error_type), allocatable, intent(out) :: error
      type(duckdb_database) :: db
      type(duckdb_connection) :: conn
      type(duckdb_result) :: ddb_result = duckdb_result()

      ! Open data in in-memory mode
      call check(error, duckdb_open(c_null_ptr, db) == duckdbsuccess)
      if (allocated(error)) return

      call check(error, duckdb_connect(db, conn) == duckdbsuccess)
      if (allocated(error)) return

      ! select true
      call check(error, &
        duckdb_query(conn, "SELECT 1=1", ddb_result) == duckdbsuccess)
      if (allocated(error)) return

      call check(error, duckdb_column_count(ddb_result) == 1)
      if (allocated(error)) return

      call check(error, duckdb_row_count(ddb_result) == 1)
      if (allocated(error)) return

      call check(error, duckdb_value_boolean(ddb_result, 0, 0))
      if (allocated(error)) return

      call check(error, .not. duckdb_value_is_null(ddb_result, 0, 0))
      if (allocated(error)) return

      ! select false
      call check(error, &
        duckdb_query(conn, "SELECT 1=0", ddb_result) == duckdbsuccess)
      if (allocated(error)) return

      call check(error, duckdb_column_count(ddb_result) == 1)
      if (allocated(error)) return

      call check(error, duckdb_row_count(ddb_result) == 1)
      if (allocated(error)) return

      call check(error, .not. duckdb_value_boolean(ddb_result, 0, 0))
      if (allocated(error)) return

      call check(error, .not. duckdb_value_is_null(ddb_result, 0, 0))
      if (allocated(error)) return

      ! select [true, false]
      ! [false
      !  true]
      call check(error, &
        duckdb_query(conn, &
        "SELECT i FROM (values (true), (false)) tbl(i) group by i order by i", &
        ddb_result) == duckdbsuccess)
      if (allocated(error)) return

      call check(error, duckdb_column_count(ddb_result) == 1, "col count")
      if (allocated(error)) return

      call check(error, duckdb_row_count(ddb_result) == 2, "row count")
      if (allocated(error)) return

      call check(error, .not. duckdb_value_boolean(ddb_result, 0, 0))
      if (allocated(error)) return

      call check(error, duckdb_value_boolean(ddb_result, 0, 1), "must be true")
      if (allocated(error)) return

      call check(error, .not. duckdb_value_is_null(ddb_result, 0, 0))
      if (allocated(error)) return

      call duckdb_destroy_result(ddb_result)
      call duckdb_disconnect(conn)
      call duckdb_close(db)

    end subroutine test_boolean

    subroutine test_multiple_insert(error)

      type(error_type), allocatable, intent(out) :: error
      type(duckdb_database) :: db
      type(duckdb_connection) :: conn
      type(duckdb_result) :: ddb_result = duckdb_result()

      ! Open data in in-memory mode
      call check(error, duckdb_open(c_null_ptr, db) == duckdbsuccess)
      if (allocated(error)) return

      call check(error, duckdb_connect(db, conn) == duckdbsuccess)
      if (allocated(error)) return

      call check(error, &
        duckdb_query(conn, "CREATE TABLE test (a INTEGER, b INTEGER);", &
        ddb_result) == duckdbsuccess, "Table creation error.")
      if (allocated(error)) return

      call check(error, &
        duckdb_query(conn, "INSERT INTO test VALUES (11, 22)", &
        ddb_result) == duckdbsuccess, "First insert error.")
      if (allocated(error)) return

      call check(error, &
        duckdb_query(conn, "INSERT INTO test VALUES (NULL, 21)", &
        ddb_result) == duckdbsuccess, "Second insert error.")
      if (allocated(error)) return

      call check(error, &
        duckdb_query(conn, "INSERT INTO test VALUES (13, 22)", &
        ddb_result) == duckdbsuccess, "Third insert error.")
      if (allocated(error)) return

      call check(error, duckdb_rows_changed(ddb_result) == 1, "rows changed")
      if (allocated(error)) return

      call check(error, &
        duckdb_query(conn, "SELECT a, b FROM test ORDER BY a", &
        ddb_result) == duckdbsuccess, "select error.")
      if (allocated(error)) return

      ! Values in the first column
      call check(error, duckdb_value_is_null(ddb_result, 0, 0), "a(0) is not null.")
      if (allocated(error)) return

      call check(error, duckdb_value_int32(ddb_result, 0, 1) == 11, "a(1) is not 11.")
      if (allocated(error)) return

      call check(error, duckdb_value_int32(ddb_result, 0, 2) == 13, "a(2) is not 13.")
      if (allocated(error)) return

      ! Values in the second column
      call check(error, duckdb_value_int32(ddb_result, 1, 0) == 21, "b(0) is not 21.")
      if (allocated(error)) return

      call check(error, duckdb_value_int32(ddb_result, 1, 1) == 22, "b(1) is not 22.")
      if (allocated(error)) return

      call check(error, duckdb_value_int32(ddb_result, 1, 2) == 22, "b(2) is not 22.")
      if (allocated(error)) return

      ! Column names
      call check(error, duckdb_column_name(ddb_result, 0) == 'a', "column name (0) is not 'a'")
      if (allocated(error)) return

      call check(error, duckdb_column_name(ddb_result, 1) == 'b', "column name (1) is not 'b'")
      if (allocated(error)) return

      call check(error, duckdb_column_name(ddb_result, 2) == 'NULL', "column name (2) is not 'NULL'")
      if (allocated(error)) return

      call check(error, &
        duckdb_query(conn, "UPDATE test SET a = 1 WHERE b=22", &
        ddb_result) == duckdbsuccess)
      if (allocated(error)) return

      call check(error, duckdb_rows_changed(ddb_result) == 2, "Number of rows affected different from 2")
      if (allocated(error)) return

      call duckdb_destroy_result(ddb_result)
      call duckdb_disconnect(conn)
      call duckdb_close(db)

    end subroutine test_multiple_insert

    subroutine test_error_conditions(error)

      type(error_type), allocatable, intent(out) :: error
      type(duckdb_result) :: res = duckdb_result()

      call check(error, .not. duckdb_value_is_null(res, 0, 0), "value_is_null?")
      if (allocated(error)) return

      call check(error, duckdb_column_type(res, 0) == duckdb_type_invalid, "col type?")
      if (allocated(error)) return

      call check(error, duckdb_column_count(res) == 0, "col count")
      if (allocated(error)) return

      call check(error, duckdb_row_count(res) == 0, "row count")
      if (allocated(error)) return

      call check(error, duckdb_rows_changed(res) == 0, "rows changed" )
      if (allocated(error)) return

      call check(error, duckdb_result_error(res) == "NULL", "result error")
      if (allocated(error)) return

      ! depreciated
      ! call check(error, .not. c_associated(duckdb_nullmask_data(result, 0)))
      ! if (allocated(error)) return
      ! call check(error, .not. c_associated(duckdb_column_data(result, 0)))
      ! if (allocated(error)) return

    end subroutine test_error_conditions

end module test_fortran_api
