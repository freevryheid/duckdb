module test_data_chunk
  use, intrinsic :: iso_c_binding
  use duckdb
  use constants
  use testdrive, only: new_unittest, unittest_type, error_type, check, skip_test
  implicit none
  private
  public :: collect_data_chunk
contains

subroutine collect_data_chunk(testsuite)

  type(unittest_type), allocatable, intent(out) :: testsuite(:)

  testsuite = [                                               &
    new_unittest("data-chunk-test", test_chunks),             &
    new_unittest("logical-types-test", test_logical_types),   &
    new_unittest("data-chunk-api-test", test_data_chunk_api)  &
  ]

end subroutine collect_data_chunk


subroutine test_chunks(error)

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

        print *, "col: ", col_idx, "row: ", row_idx, "valid: ", is_valid, "fortran: ", &
          btest(validity, row_idx)," ", btest(validity, row_idx+1)
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

end subroutine test_chunks

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

subroutine test_data_chunk_api(error)
  type(error_type), allocatable, intent(out) :: error

  type(duckdb_database) :: db
  type(duckdb_connection) :: con
  type(duckdb_result) :: result
  type(duckdb_logical_type) :: types(2), first_type
  type(duckdb_data_chunk) :: chunk

  ! Open db in in-memory mode
  call check(error, duckdb_open(c_null_ptr, db) == duckdbsuccess)
  if (allocated(error)) return

  call check(error, duckdb_connect(db, con) == duckdbsuccess)
  if (allocated(error)) return

  ! Not clear what STANDARD_VECTOR_SIZE is in the cpp test!
  ! call check(error, duckdb_vector_size() == STANDARD_VECTOR_SIZE)
  ! if (allocated(error)) return

  call check(error, duckdb_query(con, "CREATE TABLE test(i BIGINT, j SMALLINT);", &
    result) /= duckdberror)
  if (allocated(error)) return

  types(1) = duckdb_create_logical_type(duckdb_type_bigint)
  types(2) = duckdb_create_logical_type(duckdb_type_smallint)

  chunk = duckdb_create_data_chunk(types, 2)
  call check(error, c_associated(chunk%dtck))
  if (allocated(error)) return

  call check(error, duckdb_data_chunk_get_column_count(chunk) == 2)
  if (allocated(error)) return

  first_type = duckdb_vector_get_column_type(duckdb_data_chunk_get_vector(chunk, 0))
  call check(error, duckdb_get_type_id(first_type) == duckdb_type_bigint)
  if (allocated(error)) return

  call duckdb_destroy_logical_type(first_type)


! 	auto second_type = duckdb_vector_get_column_type(duckdb_data_chunk_get_vector(data_chunk, 1));
! 	REQUIRE(duckdb_get_type_id(second_type) == DUCKDB_TYPE_SMALLINT);
! 	duckdb_destroy_logical_type(&second_type);

! 	REQUIRE(duckdb_data_chunk_get_vector(data_chunk, 999) == nullptr);
! 	REQUIRE(duckdb_data_chunk_get_vector(nullptr, 0) == nullptr);
! 	REQUIRE(duckdb_vector_get_column_type(nullptr) == nullptr);

! 	REQUIRE(duckdb_data_chunk_get_size(data_chunk) == 0);
! 	REQUIRE(duckdb_data_chunk_get_size(nullptr) == 0);

! 	// use the appender to insert a value using the data chunk API

! 	duckdb_appender appender;
! 	status = duckdb_appender_create(tester.connection, nullptr, "test", &appender);
! 	REQUIRE(status == DuckDBSuccess);

! 	// append standard primitive values
! 	auto col1_ptr = (int64_t *)duckdb_vector_get_data(duckdb_data_chunk_get_vector(data_chunk, 0));
! 	*col1_ptr = 42;
! 	auto col2_ptr = (int16_t *)duckdb_vector_get_data(duckdb_data_chunk_get_vector(data_chunk, 1));
! 	*col2_ptr = 84;

! 	REQUIRE(duckdb_vector_get_data(nullptr) == nullptr);

! 	duckdb_data_chunk_set_size(data_chunk, 1);
! 	REQUIRE(duckdb_data_chunk_get_size(data_chunk) == 1);

! 	REQUIRE(duckdb_append_data_chunk(appender, data_chunk) == DuckDBSuccess);
! 	REQUIRE(duckdb_append_data_chunk(appender, nullptr) == DuckDBError);
! 	REQUIRE(duckdb_append_data_chunk(nullptr, data_chunk) == DuckDBError);

! 	// append nulls
! 	duckdb_data_chunk_reset(data_chunk);
! 	REQUIRE(duckdb_data_chunk_get_size(data_chunk) == 0);

! 	duckdb_vector_ensure_validity_writable(duckdb_data_chunk_get_vector(data_chunk, 0));
! 	duckdb_vector_ensure_validity_writable(duckdb_data_chunk_get_vector(data_chunk, 1));
! 	auto col1_validity = duckdb_vector_get_validity(duckdb_data_chunk_get_vector(data_chunk, 0));
! 	REQUIRE(duckdb_validity_row_is_valid(col1_validity, 0));
! 	duckdb_validity_set_row_validity(col1_validity, 0, false);
! 	REQUIRE(!duckdb_validity_row_is_valid(col1_validity, 0));

! 	auto col2_validity = duckdb_vector_get_validity(duckdb_data_chunk_get_vector(data_chunk, 1));
! 	REQUIRE(col2_validity);
! 	REQUIRE(duckdb_validity_row_is_valid(col2_validity, 0));
! 	duckdb_validity_set_row_validity(col2_validity, 0, false);
! 	REQUIRE(!duckdb_validity_row_is_valid(col2_validity, 0));

! 	duckdb_data_chunk_set_size(data_chunk, 1);
! 	REQUIRE(duckdb_data_chunk_get_size(data_chunk) == 1);

! 	REQUIRE(duckdb_append_data_chunk(appender, data_chunk) == DuckDBSuccess);

! 	REQUIRE(duckdb_vector_get_validity(nullptr) == nullptr);

! 	duckdb_appender_destroy(&appender);

! 	result = tester.Query("SELECT * FROM test");
! 	REQUIRE_NO_FAIL(*result);
! 	REQUIRE(result->Fetch<int64_t>(0, 0) == 42);
! 	REQUIRE(result->Fetch<int16_t>(1, 0) == 84);
! 	REQUIRE(result->IsNull(0, 1));
! 	REQUIRE(result->IsNull(1, 1));

! 	duckdb_data_chunk_reset(data_chunk);
! 	duckdb_data_chunk_reset(nullptr);
! 	REQUIRE(duckdb_data_chunk_get_size(data_chunk) == 0);

! 	duckdb_destroy_data_chunk(&data_chunk);
! 	duckdb_destroy_data_chunk(&data_chunk);

! 	duckdb_destroy_data_chunk(nullptr);

! 	duckdb_destroy_logical_type(&types[0]);
! 	duckdb_destroy_logical_type(&types[1]);
! }
end subroutine test_data_chunk_api
end module test_data_chunk