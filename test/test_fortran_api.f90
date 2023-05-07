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
        new_unittest("basic-errors-test", test_error_conditions), &
        new_unittest("basic-integers-test", test_integer_columns),&
        new_unittest("basic-real-test", test_real_columns),       &
        new_unittest("basic-date-test", test_date_columns),       &
        new_unittest("basic-time-test", test_time_columns),       &
        new_unittest("basic-blobs-test", test_blob_columns),      &
        new_unittest("basic-boolean-test", test_boolean_columns), &
        new_unittest("basic-decimal-test", test_decimal_columns)  &
        ]

    end subroutine collect_fortran_api

    subroutine test_basic(error)

      type(error_type), allocatable, intent(out) :: error
      type(duckdb_database) :: db
      type(duckdb_connection) :: conn
      type(duckdb_result) :: ddb_result = duckdb_result()
      integer(kind=int64), pointer :: data_out
      type(c_ptr) :: data_in

      ! Open data in in-memory mode
      call check(error, duckdb_open(c_null_ptr, db) == duckdbsuccess, "open database")
      if (allocated(error)) return

      call check(error, duckdb_connect(db, conn) == duckdbsuccess, "connect database")
      if (allocated(error)) return

      call check(error, duckdb_query(conn, "SET default_null_order='nulls_first'", &
        ddb_result) == duckdbsuccess)
      if (allocated(error)) return

      call check(error, &
        duckdb_query(conn, "SELECT CAST(42 AS BIGINT);", ddb_result) == duckdbsuccess, &
        "query database")
      if (allocated(error)) return

      call check(error, duckdb_column_type(ddb_result, 0) == duckdb_type_bigint, &
        "database column type")
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

      call check(error, .not. duckdb_value_is_null(ddb_result, 0, 0), "col 0 row 0 not null")
      if (allocated(error)) return

      ! Out of range fetch 
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

      call duckdb_destroy_result(res)
    end subroutine test_error_conditions

    subroutine test_integer_columns(error)

      type(error_type), allocatable, intent(out) :: error
      type(duckdb_database) :: db
      type(duckdb_connection) :: conn
      type(duckdb_result) :: result = duckdb_result()

      character(len=8) :: types(5)
      integer :: i

      ! Open data in in-memory mode
      call check(error, duckdb_open(c_null_ptr, db) == duckdbsuccess, "open database")
      if (allocated(error)) return

      call check(error, duckdb_connect(db, conn) == duckdbsuccess, "connect database")
      if (allocated(error)) return

      call check(error, duckdb_query(conn, "SET default_null_order='nulls_first'", &
        result) == duckdbsuccess, "null order query")
      if (allocated(error)) return

      types = ["TINYINT ",  "SMALLINT",  "INTEGER ",  "BIGINT  ", "HUGEINT "]

      do i = 1, size(types,1)
        ! create the table and insert values
        call check(error, duckdb_query(conn, "BEGIN TRANSACTION", &
          result) == duckdbsuccess, "begin transaction")
        if (allocated(error)) return

        call check(error, duckdb_query(conn, "CREATE TABLE integers(i "//trim(types(i))//")", &
          result) == duckdbsuccess, "create "//trim(types(i))//" table")
        if (allocated(error)) return

        call check(error, duckdb_query(conn, "INSERT INTO integers VALUES (1), (NULL)", &
          result) == duckdbsuccess, "insert "//trim(types(i))//" values")
        if (allocated(error)) return

        call check(error, duckdb_query(conn, "SELECT * FROM integers ORDER BY i", &
          result) == duckdbsuccess, "select "//trim(types(i))//" values")
        if (allocated(error)) return

        call check(error, duckdb_value_is_null(result, 0, 0), &
          trim(types(i))//": 0 null")
        if (allocated(error)) return        
        call check(error, duckdb_value_int8(result, 0, 0) == 0, &
          trim(types(i))//": 0 int8")
        if (allocated(error)) return        
        call check(error, duckdb_value_int16(result, 0, 0) == 0, &
          trim(types(i))//": 0 int16")
        if (allocated(error)) return        
        call check(error, duckdb_value_int32(result, 0, 0) == 0, &
          trim(types(i))//": 0 int32")
        if (allocated(error)) return        
        call check(error, duckdb_value_int64(result, 0, 0) == 0, &
          trim(types(i))//": 0 int64")
        if (allocated(error)) return        
        call check(error, duckdb_hugeint_to_double(duckdb_value_hugeint(result, 0, 0)), &
          0.0_real64, trim(types(i))//": 0 hugeint")
        if (allocated(error)) return        
        call check(error, duckdb_string_to_character(duckdb_value_string(result, 0, 0)) &
          == "", trim(types(i))//": 0 string")
        if (allocated(error)) return
        call check(error, duckdb_value_float(result, 0, 0), 0.0_real32, &
          trim(types(i))//": 0 float")
        if (allocated(error)) return
        call check(error, duckdb_value_double(result, 0, 0), 0.0_real64, &
          trim(types(i))//": 0 double")
        if (allocated(error)) return

        call check(error, .not. duckdb_value_is_null(result, 0, 1), &
          trim(types(i))//": 1 null")
        if (allocated(error)) return        
        call check(error, duckdb_value_int8(result, 0, 1) == 1, &
          trim(types(i))//": 1 int8")
        if (allocated(error)) return        
        call check(error, duckdb_value_int16(result, 0, 1) == 1, &
          trim(types(i))//": 1 int16")
        if (allocated(error)) return        
        call check(error, duckdb_value_int32(result, 0, 1) == 1, &
          trim(types(i))//": 1 int32")
        if (allocated(error)) return        
        call check(error, duckdb_value_int64(result, 0, 1) == 1, &
          trim(types(i))//": 1 int64")
        if (allocated(error)) return      
        block   
          type(duckdb_hugeint) :: hi 
          hi = duckdb_value_hugeint(result, 0, 1)
          call check(error, duckdb_hugeint_to_double(hi), 1.0_real64, &
            trim(types(i))//": 1 hugeint")
          if (allocated(error)) return        
        end block 
        call check(error, duckdb_string_to_character(duckdb_value_string(result, 0, 1)) == "1", &
          trim(types(i))//": 1 string")
        if (allocated(error)) return
        call check(error, duckdb_value_float(result, 0, 1), 1.0_real32, &
          trim(types(i))//": 1 float")
        if (allocated(error)) return
        call check(error, duckdb_value_double(result, 0, 1), 1.0_real64, &
          trim(types(i))//": 1 double")
        if (allocated(error)) return

        call check(error, duckdb_query(conn, "ROLLBACK", &
          result) == duckdbsuccess, trim(types(i))//" rollback")
        if (allocated(error)) return        
      enddo

      call duckdb_destroy_result(result)
      call duckdb_disconnect(conn)
      call duckdb_close(db)
      
    end subroutine test_integer_columns

    subroutine test_real_columns(error)

      type(error_type), allocatable, intent(out) :: error
      type(duckdb_database) :: db
      type(duckdb_connection) :: conn
      type(duckdb_result) :: result = duckdb_result()

      character(len=6) :: types(2)
      integer :: i

      ! Open data in in-memory mode
      call check(error, duckdb_open(c_null_ptr, db) == duckdbsuccess, "open database")
      if (allocated(error)) return

      call check(error, duckdb_connect(db, conn) == duckdbsuccess, "connect database")
      if (allocated(error)) return

      call check(error, duckdb_query(conn, "SET default_null_order='nulls_first'", &
        result) == duckdbsuccess, "null order query")
      if (allocated(error)) return

      types = ["REAL  ",  "DOUBLE"]

      do i = 1, size(types,1)
        ! create the table and insert values
        call check(error, duckdb_query(conn, "BEGIN TRANSACTION", &
          result) == duckdbsuccess, "begin transaction")
        if (allocated(error)) return

        call check(error, duckdb_query(conn, "CREATE TABLE doubles(i "//trim(types(i))//")", &
          result) == duckdbsuccess, "create "//trim(types(i))//" table")
        if (allocated(error)) return

        call check(error, duckdb_query(conn, "INSERT INTO doubles VALUES (1), (NULL)", &
          result) == duckdbsuccess, "insert "//trim(types(i))//" values")
        if (allocated(error)) return

        call check(error, duckdb_query(conn, "SELECT * FROM doubles ORDER BY i", &
          result) == duckdbsuccess, "select "//trim(types(i))//" values")
        if (allocated(error)) return

        call check(error, duckdb_value_is_null(result, 0, 0), &
          trim(types(i))//": 0 null")
        if (allocated(error)) return        
        call check(error, duckdb_value_int8(result, 0, 0) == 0, &
          trim(types(i))//": 0 int8")
        if (allocated(error)) return        
        call check(error, duckdb_value_int16(result, 0, 0) == 0, &
          trim(types(i))//": 0 int16")
        if (allocated(error)) return        
        call check(error, duckdb_value_int32(result, 0, 0) == 0, &
          trim(types(i))//": 0 int32")
        if (allocated(error)) return        
        call check(error, duckdb_value_int64(result, 0, 0) == 0, &
          trim(types(i))//": 0 int64")
        if (allocated(error)) return           
        call check(error, duckdb_string_to_character(duckdb_value_string(result, 0, 0)) == "", &
          trim(types(i))//": 0 string")
        if (allocated(error)) return
        call check(error, duckdb_value_float(result, 0, 0), 0.0_real32, &
          trim(types(i))//": 0 float")
        if (allocated(error)) return
        call check(error, duckdb_value_double(result, 0, 0), 0.0_real64, &
          trim(types(i))//": 0 double")
        if (allocated(error)) return

        call check(error, .not. duckdb_value_is_null(result, 0, 1), &
          trim(types(i))//": 1 null")
        if (allocated(error)) return        
        call check(error, duckdb_value_int8(result, 0, 1) == 1, &
          trim(types(i))//": 1 int8")
        if (allocated(error)) return        
        call check(error, duckdb_value_int16(result, 0, 1) == 1, &
          trim(types(i))//": 1 int16")
        if (allocated(error)) return        
        call check(error, duckdb_value_int32(result, 0, 1) == 1, &
          trim(types(i))//": 1 int32")
        if (allocated(error)) return        
        call check(error, duckdb_value_int64(result, 0, 1) == 1, &
          trim(types(i))//": 1 int64")
        if (allocated(error)) return      
        call check(error, duckdb_value_float(result, 0, 1), 1.0_real32, &
          trim(types(i))//": 1 float")
        if (allocated(error)) return
        call check(error, duckdb_value_double(result, 0, 1), 1.0_real64, &
          trim(types(i))//": 1 double")
        if (allocated(error)) return

        call check(error, duckdb_query(conn, "ROLLBACK", &
          result) == duckdbsuccess, trim(types(i))//" rollback")
        if (allocated(error)) return        
      enddo

      call duckdb_destroy_result(result)
      call duckdb_disconnect(conn)
      call duckdb_close(db)

    end subroutine test_real_columns

    subroutine test_date_columns(error)

      type(error_type), allocatable, intent(out) :: error
      type(duckdb_database) :: db
      type(duckdb_connection) :: conn
      type(duckdb_result) :: result = duckdb_result()

      type(duckdb_date_struct) :: date

      ! Open data in in-memory mode
      call check(error, duckdb_open(c_null_ptr, db) == duckdbsuccess, "open database")
      if (allocated(error)) return

      call check(error, duckdb_connect(db, conn) == duckdbsuccess, "connect database")
      if (allocated(error)) return

      call check(error, duckdb_query(conn, "SET default_null_order='nulls_first'", &
        result) == duckdbsuccess, "null order query")
      if (allocated(error)) return

      ! create the table and insert values
      call check(error, duckdb_query(conn, "BEGIN TRANSACTION", &
        result) == duckdbsuccess, "begin transaction")
      if (allocated(error)) return

      call check(error, duckdb_query(conn, "CREATE TABLE dates(d DATE)", &
        result) == duckdbsuccess, "create dates table")
      if (allocated(error)) return

      call check(error, duckdb_query(conn, &
        "INSERT INTO dates VALUES ('1992-09-20'), (NULL), ('30000-09-20')", &
        result) == duckdbsuccess, "insert date values")
      if (allocated(error)) return

      call check(error, duckdb_query(conn, "SELECT * FROM dates ORDER BY d", &
        result) == duckdbsuccess, "select dates values")
      if (allocated(error)) return

      call check(error, duckdb_value_is_null(result, 0, 0), &
        "dates: 0 null")
      if (allocated(error)) return      
      date = duckdb_from_date(duckdb_value_date(result, 0, 1))
      call check(error, date%year == 1992, "dates: 1 year")
      if (allocated(error)) return      
      call check(error, date%month == 9, "dates: 1 month")
      if (allocated(error)) return    
      call check(error, date%day == 20, "dates: 1 day")
      if (allocated(error)) return                         
      call check(error, duckdb_string_to_character(duckdb_value_string(result, 0, 1)), &
        "1992-09-20", "dates: 1 string")
      if (allocated(error)) return
      date = duckdb_from_date(duckdb_value_date(result, 0, 2))
      call check(error, date%year == 30000, "dates: 2 year")
      if (allocated(error)) return      
      call check(error, date%month == 9, "dates: 2 month")
      if (allocated(error)) return    
      call check(error, date%day == 20, "dates: 2 day")
      if (allocated(error)) return    
      call check(error, duckdb_string_to_character(duckdb_value_string(result, 0, 2)), &
        "30000-09-20", "dates: 2 string")

      call duckdb_destroy_result(result)
      call duckdb_disconnect(conn)
      call duckdb_close(db)

    end subroutine test_date_columns

    subroutine test_time_columns(error)

      type(error_type), allocatable, intent(out) :: error
      type(duckdb_database) :: db
      type(duckdb_connection) :: conn
      type(duckdb_result) :: result = duckdb_result()

      type(duckdb_time_struct) :: time_val

      ! Open data in in-memory mode
      call check(error, duckdb_open(c_null_ptr, db) == duckdbsuccess, "open database")
      if (allocated(error)) return

      call check(error, duckdb_connect(db, conn) == duckdbsuccess, "connect database")
      if (allocated(error)) return

      call check(error, duckdb_query(conn, "SET default_null_order='nulls_first'", &
        result) == duckdbsuccess, "null order query")
      if (allocated(error)) return

      call check(error, duckdb_query(conn, "CREATE TABLE times(d TIME)", &
        result) == duckdbsuccess, "create times table")
      if (allocated(error)) return

      call check(error, duckdb_query(conn, &
        "INSERT INTO times VALUES ('12:00:30.1234'), (NULL), ('02:30:01')", &
        result) == duckdbsuccess, "insert time values")
      if (allocated(error)) return

      call check(error, duckdb_query(conn, "SELECT * FROM times ORDER BY d", &
        result) == duckdbsuccess, "select time values")
      if (allocated(error)) return

      call check(error, duckdb_value_is_null(result, 0, 0), &
        "time: 0 null")
      if (allocated(error)) return  
      time_val = duckdb_from_time(duckdb_value_time(result, 0, 1))
      call check(error, time_val%hour == 2, "time: 1 hour")
      if (allocated(error)) return      
      call check(error, time_val%min == 30, "time: 1 min")
      if (allocated(error)) return    
      call check(error, time_val%sec == 1, "time: 1 sec")
      if (allocated(error)) return  
      call check(error, time_val%micros == 0, "time: 1 micros")
      if (allocated(error)) return                         
      call check(error, duckdb_string_to_character(duckdb_value_string(result, 0, 1)), &
      '02:30:01', "time: 1 string")
      if (allocated(error)) return
      time_val = duckdb_from_time(duckdb_value_time(result, 0, 2))
      call check(error, time_val%hour == 12, "time: 2 hour")
      if (allocated(error)) return      
      call check(error, time_val%min == 0, "time: 2 min")
      if (allocated(error)) return    
      call check(error, time_val%sec == 30, "time: 2 sec")
      if (allocated(error)) return  
      call check(error, time_val%micros == 123400, "time: 2 micros")
      if (allocated(error)) return                         
      call check(error, duckdb_string_to_character(duckdb_value_string(result, 0, 2)), &
      '12:00:30.1234', "time: 2 string")

      call duckdb_destroy_result(result)
      call duckdb_disconnect(conn)
      call duckdb_close(db)

    end subroutine test_time_columns

    subroutine test_blob_columns(error)

      type(error_type), allocatable, intent(out) :: error
      type(duckdb_database) :: db
      type(duckdb_connection) :: conn
      type(duckdb_result) :: result = duckdb_result()

      ! Open data in in-memory mode
      call check(error, duckdb_open(c_null_ptr, db) == duckdbsuccess)
      if (allocated(error)) return

      call check(error, duckdb_connect(db, conn) == duckdbsuccess)
      if (allocated(error)) return

      call check(error, &
        duckdb_query(conn, "CREATE TABLE blobs(b BLOB)", &
        result) == duckdbsuccess, "blob table create error.")
      if (allocated(error)) return

      call check(error, &
        duckdb_query(conn, "INSERT INTO blobs VALUES ('hello\\x12world'), ('\\x00'), (NULL)", &
        result) == duckdbsuccess, "blob table insert error.")
      if (allocated(error)) return

      call check(error, &
        duckdb_query(conn, "SELECT * FROM blobs", result) == duckdbsuccess, &
        "blob table select error.")
      if (allocated(error)) return     
      
      call check(error, .not. duckdb_value_is_null(result, 0, 0), "result(0,0) is null")
      if (allocated(error)) return     
      
      block 
        type(duckdb_blob) :: blob 
        blob = duckdb_value_blob(result, 0, 0)
        call check(error, blob%size == 11, "Blob size mismatch")
        if (allocated(error)) return 
      end block

      call duckdb_destroy_result(result)
      call duckdb_disconnect(conn)
      call duckdb_close(db)

    end subroutine test_blob_columns

    subroutine test_boolean_columns(error)

      type(error_type), allocatable, intent(out) :: error
      type(duckdb_database) :: db
      type(duckdb_connection) :: conn
      type(duckdb_result) :: result = duckdb_result()

      type(duckdb_time_struct) :: time_val

      ! Open data in in-memory mode
      call check(error, duckdb_open(c_null_ptr, db) == duckdbsuccess, "open database")
      if (allocated(error)) return

      call check(error, duckdb_connect(db, conn) == duckdbsuccess, "connect database")
      if (allocated(error)) return

      call check(error, duckdb_query(conn, "SET default_null_order='nulls_first'", &
        result) == duckdbsuccess, "null order query")
      if (allocated(error)) return

      call check(error, duckdb_query(conn, "CREATE TABLE booleans(b BOOLEAN)", &
        result) == duckdbsuccess, "create boolean table")
      if (allocated(error)) return

      call check(error, duckdb_query(conn, &
        "INSERT INTO booleans VALUES (42 > 60), (42 > 20), (42 > NULL)", &
        result) == duckdbsuccess, "insert boolean values")
      if (allocated(error)) return

      call check(error, duckdb_query(conn, "SELECT * FROM booleans ORDER BY b", &
        result) == duckdbsuccess, "select boolean values")
      if (allocated(error)) return

      call check(error, duckdb_value_is_null(result, 0, 0), &
        "boolean: 0 null")
      if (allocated(error)) return  
      call check(error, .not. duckdb_value_boolean(result, 0, 0), "boolean: 1")
      if (allocated(error)) return      
      call check(error, .not. duckdb_value_boolean(result, 0, 1), "boolean: 2")
      if (allocated(error)) return 
      call check(error, duckdb_value_boolean(result, 0, 2), "boolean: 3")
      if (allocated(error)) return                        
      call check(error, duckdb_string_to_character(duckdb_value_string(result, 0, 2)), &
      "true", "boolean: 3 string")
      if (allocated(error)) return

      call duckdb_destroy_result(result)
      call duckdb_disconnect(conn)
      call duckdb_close(db)

    end subroutine test_boolean_columns

    subroutine test_decimal_columns(error)

      type(error_type), allocatable, intent(out) :: error
      type(duckdb_database) :: db
      type(duckdb_connection) :: conn
      type(duckdb_result) :: result = duckdb_result()

      ! Open data in in-memory mode
      call check(error, duckdb_open(c_null_ptr, db) == duckdbsuccess)
      if (allocated(error)) return

      call check(error, duckdb_connect(db, conn) == duckdbsuccess)
      if (allocated(error)) return

      call check(error, &
        duckdb_query(conn, "CREATE TABLE decimals(dec DECIMAL(18, 4) NULL)", &
        result) == duckdbsuccess, "decimal table create error.")
      if (allocated(error)) return

      call check(error, &
        duckdb_query(conn, "INSERT INTO decimals VALUES (NULL), (12.3)", &
        result) == duckdbsuccess, "decimal table insert error.")
      if (allocated(error)) return

      call check(error, &
        duckdb_query(conn, "SELECT * FROM decimals ORDER BY dec", result) == duckdbsuccess, &
        "decimal table select error.")
      if (allocated(error)) return     
      
      call check(error, duckdb_value_is_null(result, 0, 0), "result(0,0) is null")
      if (allocated(error)) return     
      
      block 
        type(duckdb_decimal) :: decimal 
        decimal = duckdb_value_decimal(result, 0, 1)
        call check(error, duckdb_decimal_to_double(decimal), 12.3_real64, &
          "Decimal value mismatch")
        if (allocated(error)) return 
      end block

      call duckdb_destroy_result(result)
      call duckdb_disconnect(conn)
      call duckdb_close(db)

    end subroutine test_decimal_columns    
end module test_fortran_api
