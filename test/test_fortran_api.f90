module test_fortran_api

  ! https://github.com/duckdb/duckdb/blob/master/test/api/capi/test_capi.cpp
  use, intrinsic :: iso_c_binding
  use, intrinsic :: iso_fortran_env, only : int8, int16, int32, int64, real32, real64, real128
  ! use stdlib_bitsets
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
        new_unittest("parquet-api-test", test_parquet),           &
        new_unittest("data-chunk-test", test_data_chunk),         &
        new_unittest("logical-types-test", test_logical_types)]

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

    subroutine test_parquet(error)

      type(error_type), allocatable, intent(out) :: error
      type(duckdb_database) :: db
      type(duckdb_connection) :: con
      type(duckdb_result) :: result = duckdb_result()
      integer :: i, j, nc, nr

      ! Open data in in-memory mode
      call check(error, duckdb_open(c_null_ptr, db) == duckdbsuccess)
      if (allocated(error)) return

      call check(error, duckdb_connect(db, con) == duckdbsuccess)
      if (allocated(error)) return

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
      call duckdb_disconnect(con)
      call duckdb_close(db)

    end subroutine test_parquet

    subroutine test_data_chunk(error)

      type(error_type), allocatable, intent(out) :: error
      type(duckdb_database) :: db
      type(duckdb_connection) :: con
      type(duckdb_result) :: result = duckdb_result()
      type(duckdb_data_chunk) :: chunk
      type(duckdb_vector) :: vector

      integer :: col_count, col_idx
      integer :: chunk_count, chunk_idx
      integer :: row_count, row_idx
      integer(kind=int64) :: validity

      ! type(bitset_64) :: set0
      character(len=64) :: bit_string
      logical :: is_valid

      ! Open db in in-memory mode
      call check(error, duckdb_open(c_null_ptr, db) == duckdbsuccess)
      if (allocated(error)) return

      call check(error, duckdb_connect(db, con) == duckdbsuccess)
      if (allocated(error)) return

      ! Create a table with 40 columns
      call check(error, duckdb_query(con, &
        "CREATE TABLE foo (c00 varchar, c01 varchar, c02 varchar, c03 varchar, c04 varchar, c05 "     // &
        "varchar, c06 varchar, c07 varchar, c08 varchar, c09 varchar, c10 varchar, c11 varchar, c12 " // &
        "varchar, c13 varchar, c14 varchar, c15 varchar, c16 varchar, c17 varchar, c18 varchar, c19 " // &
        "varchar, c20 varchar, c21 varchar, c22 varchar, c23 varchar, c24 varchar, c25 varchar, c26 " // &
        "varchar, c27 varchar, c28 varchar, c29 varchar, c30 varchar, c31 varchar, c32 varchar, c33 " // &
        "varchar, c34 varchar, c35 varchar, c36 varchar, c37 varchar, c38 varchar, c39 varchar);",       &
        result) /= duckdberror)
      if (allocated(error)) return

      ! Get table info for the created table
      call check(error, duckdb_query(con, "PRAGMA table_info(foo);", result) /= duckdberror)
      if (allocated(error)) return

      ! Columns ({cid, name, type, notnull, dflt_value, pk}}
      col_count = duckdb_column_count(result)
      call check(error, col_count == 6)

      chunk_count = duckdb_result_chunk_count(result)
      ! print *, "chunk_count: ", chunk_count
      ! print *, "vector_size: ", duckdb_vector_size() ! 2048 vs 1024

      ! Loop over the produced chunks
      do chunk_idx = 0, chunk_count - 1
        chunk = duckdb_result_get_chunk(result, chunk_idx)
        row_count = duckdb_data_chunk_get_size(chunk)

        do row_idx = 0, row_count - 1
          do col_idx = 0, col_count - 1
            ! Get the column
            vector = duckdb_data_chunk_get_vector(chunk, col_idx)
            validity = duckdb_vector_get_validity(vector)
            is_valid = duckdb_validity_row_is_valid(validity, row_idx)

            print *, "col: ", col_idx, "row: ", row_idx, "valid: ", is_valid
            write(bit_string, fmt='(B0)') validity
            print *, bit_string

            if (col_idx == 4) then
              ! 'dflt_value' column
              call check(error, is_valid .eqv. .false.)
              if (allocated(error)) return
            endif

          end do

        end do

        call duckdb_destroy_data_chunk(chunk)

      end do

      call duckdb_destroy_result(result)
      call duckdb_disconnect(con)
      call duckdb_close(db)

    end subroutine test_data_chunk


    subroutine test_logical_types(error)

      type(error_type), allocatable, intent(out) :: error
      type(duckdb_logical_type) :: type
      type(duckdb_logical_type) :: elem_type, elem_type_dup, list_type
      type(duckdb_logical_type) :: key_type, value_type, map_type
      type(duckdb_logical_type) :: key_type_dup, value_type_dup !, map_type

      type = duckdb_create_logical_type(duckdb_type_bigint)
      call check(error, duckdb_get_type_id(type) == duckdb_type_bigint)
      if (allocated(error)) return

      call duckdb_destroy_logical_type(type)
      call duckdb_destroy_logical_type(type) ! Not sure why it's called twice in the cpp code.

      ! list type
      elem_type = duckdb_create_logical_type(duckdb_type_integer)
      list_type = duckdb_create_list_type(elem_type)
      call check(error, duckdb_get_type_id(list_type) == duckdb_type_list)
      if (allocated(error)) return

      elem_type_dup = duckdb_list_type_child_type(list_type)
      ! Not possible to compare the two derived types in fortran without writing a custom operator?
      ! REQUIRE(elem_type_dup != elem_type);
      ! call check(error, elem_type_dup /= elem_type)
      ! if (allocated(error)) return
      call check(error, duckdb_get_type_id(elem_type_dup) == duckdb_get_type_id(elem_type))
      if (allocated(error)) return

      call duckdb_destroy_logical_type(elem_type)
      call duckdb_destroy_logical_type(elem_type_dup)
      call duckdb_destroy_logical_type(list_type)

      ! map type
      key_type = duckdb_create_logical_type(duckdb_type_smallint)
      value_type = duckdb_create_logical_type(duckdb_type_double)
      map_type = duckdb_create_map_type(key_type, value_type)
      call check(error, duckdb_get_type_id(map_type) == duckdb_type_map)
      if (allocated(error)) return

      key_type_dup = duckdb_map_type_key_type(map_type)
      value_type_dup = duckdb_map_type_value_type(map_type)
      ! Not possible to compare the two derived types in fortran without writing a custom operator?
      ! REQUIRE(key_type_dup != key_type);
      ! REQUIRE(value_type_dup != value_type);
      ! call check(error, key_type_dup /= key_type)
      ! if (allocated(error)) return
      ! call check(error, value_type_dup /= value_type)
      ! if (allocated(error)) return

      call check(error, duckdb_get_type_id(key_type_dup) == duckdb_get_type_id(key_type))
      if (allocated(error)) return
      call check(error, duckdb_get_type_id(value_type_dup) == duckdb_get_type_id(value_type))
      if (allocated(error)) return

      call duckdb_destroy_logical_type(key_type)
      call duckdb_destroy_logical_type(value_type)
      call duckdb_destroy_logical_type(map_type)
      call duckdb_destroy_logical_type(key_type_dup)
      call duckdb_destroy_logical_type(value_type_dup)

    end subroutine test_logical_types

end module test_fortran_api
