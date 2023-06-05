module test_data_chunk

  use, intrinsic :: iso_c_binding
  use duckdb
  use constants
  use util
  use testdrive, only: new_unittest, unittest_type, error_type, check, skip_test

  implicit none

  private
  public :: collect_data_chunk

contains

  subroutine collect_data_chunk(testsuite)

    type(unittest_type), allocatable, intent(out) :: testsuite(:)

    testsuite = [                                                               &
      new_unittest("data-chunk-test", test_chunks),                             &
      new_unittest("logical-types-test", test_logical_types),                   &
      new_unittest("data-chunk-api-test", test_data_chunk_api),                 &
      new_unittest("varchar-chunk-test", test_data_chunk_varchar_result_fetch)  &
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
    integer(kind=int64), pointer :: validity
    type(c_ptr) :: ptr

    character(len=64) :: bit_string
    logical :: is_valid

    ! Open db in in-memory mode
    call check(error, duckdb_open("", db) == duckdbsuccess)
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
          ptr = duckdb_vector_get_validity(vector)
          call c_f_pointer(ptr, validity)
          is_valid = duckdb_validity_row_is_valid(validity, row_idx)

          ! print *, "col: ", col_idx, "row: ", row_idx, "valid: ", is_valid, "fortran: ", &
          !   btest(validity, row_idx)," ", btest(validity, row_idx+1)
          ! write(bit_string, fmt='(B0)') validity
          ! print *, bit_string

          if (col_idx == 4) then
            ! 'dflt_value' column
            call check(error, is_valid .eqv. .false.)
            if (allocated(error)) return
          end if

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
    type(duckdb_result) :: result = duckdb_result()
    type(duckdb_logical_type) :: types(2), first_type, second_type
    type(duckdb_data_chunk) :: chunk != duckdb_data_chunk()
    type(duckdb_data_chunk) :: c != duckdb_data_chunk()
    type(duckdb_vector) :: v, vec1, vec2
    type(duckdb_appender) :: appender, a

    type(c_ptr) :: col1_ptr, col2_ptr

    integer(kind=int64), pointer:: col1_val
    integer(kind=int16), pointer:: col2_val

    integer(kind=int64), pointer :: col1_validity, col2_validity

    character(len=64) :: bit_string

    ! Open db in in-memory mode
    call check(error, duckdb_open("", db) == duckdbsuccess)
    if (allocated(error)) return

    call check(error, duckdb_connect(db, con) == duckdbsuccess)
    if (allocated(error)) return

    call check(error, duckdb_vector_size() == STANDARD_VECTOR_SIZE)
    if (allocated(error)) return

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

    vec1 = duckdb_data_chunk_get_vector(chunk, 0)
    vec2 = duckdb_data_chunk_get_vector(chunk, 1)

    ! first_type = duckdb_vector_get_column_type(duckdb_data_chunk_get_vector(chunk, 0))
    first_type = duckdb_vector_get_column_type(vec1)
    call check(error, duckdb_get_type_id(first_type) == duckdb_type_bigint)
    if (allocated(error)) return

    call duckdb_destroy_logical_type(first_type)

    ! second_type = duckdb_vector_get_column_type(duckdb_data_chunk_get_vector(chunk, 1))
    second_type = duckdb_vector_get_column_type(vec2)
    call check(error, duckdb_get_type_id(second_type) == duckdb_type_smallint)
    if (allocated(error)) return

    call duckdb_destroy_logical_type(second_type)

    v = duckdb_data_chunk_get_vector(chunk, 999)
    call check(error,.not. c_associated(v%vctr))
    if (allocated(error)) return

    v = duckdb_data_chunk_get_vector(c, 0)
    call check(error,.not. c_associated(v%vctr))
    if (allocated(error)) return

    first_type = duckdb_vector_get_column_type(v)
    call check(error,.not. c_associated(first_type%lglt))
    if (allocated(error)) return

    call check(error, duckdb_data_chunk_get_size(chunk) == 0)
    if (allocated(error)) return

    call check(error, duckdb_data_chunk_get_size(c) == 0)
    if (allocated(error)) return

    ! use the appender to insert a value using the data chunk API
    call check(error, duckdb_appender_create(con, "", "test", appender) == duckdbsuccess, "create appender")
    if (allocated(error)) return

    ! append standard primitive values
    ! NOTE: chunk was created earlier as [bigint, smallint]
    col1_ptr = duckdb_vector_get_data(vec1)
    call c_f_pointer(col1_ptr, col1_val)
    col1_val = 42_int64

    col2_ptr = duckdb_vector_get_data(vec2)
    call c_f_pointer(col2_ptr, col2_val)
    col2_val = 84_int16

    call check(error,.not. c_associated(duckdb_vector_get_data(v)))
    if (allocated(error)) return

    call duckdb_data_chunk_set_size(chunk, 1) ! set chunk to 1 tuple
    call check(error, duckdb_data_chunk_get_size(chunk) == 1, "Mismatching chunk size.")
    if (allocated(error)) return

    call check(error, duckdb_append_data_chunk(appender, chunk) == duckdbsuccess, "Append chunk error.")
    if (allocated(error)) return

    call check(error, duckdb_append_data_chunk(appender, c) == duckdberror, "Append null chunk.")
    if (allocated(error)) return

    call check(error, duckdb_append_data_chunk(a, chunk) == duckdberror, "Append to null appender.")
    if (allocated(error)) return

    ! append nulls
    call check(error, duckdb_data_chunk_get_size(chunk) == 1)
    call duckdb_data_chunk_reset(chunk)
    call check(error, duckdb_data_chunk_get_size(chunk) == 0)
    if (allocated(error)) return

    call duckdb_vector_ensure_validity_writable(vec1)
    call duckdb_vector_ensure_validity_writable(vec2)

    col1_ptr = duckdb_vector_get_validity(vec1)
    call c_f_pointer(col1_ptr, col1_validity)
    call check(error, duckdb_validity_row_is_valid(col1_validity, 0))
    if (allocated(error)) return
    call duckdb_validity_set_row_validity(col1_validity, 0, .false.)
    call check(error,.not. duckdb_validity_row_is_valid(col1_validity, 0), "Failed to invalidate row 0")
    if (allocated(error)) return

    col2_ptr = duckdb_vector_get_validity(vec2)
    call c_f_pointer(col2_ptr, col2_validity)
    call check(error, duckdb_validity_row_is_valid(col2_validity, 0))
    if (allocated(error)) return
    call duckdb_validity_set_row_validity(col2_validity, 0, .false.)
    call check(error,.not. duckdb_validity_row_is_valid(col2_validity, 0))
    if (allocated(error)) return

    call duckdb_data_chunk_set_size(chunk, 1)
    call check(error, duckdb_data_chunk_get_size(chunk) == 1)
    if (allocated(error)) return

    call check(error, duckdb_append_data_chunk(appender, chunk) == duckdbsuccess)
    if (allocated(error)) return

    call check(error, duckdb_appender_destroy(appender) == duckdbsuccess)
    if (allocated(error)) return

    call check(error, duckdb_query(con, "SELECT * FROM test", result) /= duckdberror)
    if (allocated(error)) return

    call check(error,.not. duckdb_value_is_null(result, 0, 0), "col1 row1 null")
    if (allocated(error)) return

    call check(error,.not. duckdb_value_is_null(result, 1, 0), "col1 row1 null")
    if (allocated(error)) return

    call check(error, duckdb_value_int64(result, 0, 0) == col1_val, "col1 row1 value")
    if (allocated(error)) return
    call check(error, duckdb_value_int16(result, 1, 0) == col2_val, "col2 row1 value")
    if (allocated(error)) return

    call check(error, duckdb_value_is_null(result, 0, 1), "col1 row2 null")
    if (allocated(error)) return
    call check(error, duckdb_value_is_null(result, 1, 1), "col2 row2 null")
    if (allocated(error)) return

    call duckdb_data_chunk_reset(chunk)
    call duckdb_data_chunk_reset(c)
    call check(error, duckdb_data_chunk_get_size(chunk) == 0)
    if (allocated(error)) return

    call duckdb_destroy_data_chunk(chunk)
    call duckdb_destroy_data_chunk(chunk)
    call duckdb_destroy_data_chunk(c)

    call duckdb_destroy_logical_type(types(1))
    call duckdb_destroy_logical_type(types(2))

  end subroutine test_data_chunk_api

  subroutine test_data_chunk_varchar_result_fetch(error)

    type(error_type), allocatable, intent(out) :: error
    type(duckdb_database) :: db
    type(duckdb_connection) :: conn
    type(duckdb_result) :: result = duckdb_result()
    type(duckdb_data_chunk) :: chunk
    type(duckdb_vector) :: vector
    type(c_ptr) :: vector_ptr
    type(c_ptr) :: string_data
    integer(kind=int64), pointer :: vectr_validity
    character(len=*), parameter :: varchar_test_query = "select case when i != 0&
      &and i % 42 = 0 then NULL else repeat(chr((65 + (i % 26))::INTEGER), (4 + &
      &(i % 12))) end from range(5000) tbl(i);"
    integer :: tuple_index, chunk_amount, chunk_index, tuples_in_chunk, i
    integer :: expected_length
    character(len=1) :: expected_character
    integer(kind=int64), pointer :: vector_validity
    character(len=:), allocatable :: f_string
    
    if (duckdb_vector_size() < 64) return

    ! Open db in in-memory mode
    call check(error, duckdb_open("", db) == duckdbsuccess)
    if (allocated(error)) return

    call check(error, duckdb_connect(db, conn) == duckdbsuccess)
    if (allocated(error)) return

    call check(error, duckdb_vector_size() == STANDARD_VECTOR_SIZE)
    if (allocated(error)) return

    call check(error, duckdb_query(conn, varchar_test_query, result) /= duckdberror)
    if (allocated(error)) return

    call check(error, duckdb_column_count(result) == 1, "Invalid column count")
    if (allocated(error)) return

    call check(error, duckdb_row_count(result) == 5000, "Invalid row count")
    if (allocated(error)) return

    call check(error, duckdb_result_error(result) == "", "Error in result")
    if (allocated(error)) return

    ! Calculate Expected Chunk Count
    block 
      integer :: expected_chunk_count
      expected_chunk_count = (5000 / STANDARD_VECTOR_SIZE) 
      if (mod(5000, STANDARD_VECTOR_SIZE) /= 0) expected_chunk_count = expected_chunk_count + 1
      call check(error, duckdb_result_chunk_count(result) == expected_chunk_count, &
        "Invalid chunk count")
      if (allocated(error)) return
    end block 

    ! Fetch and Process Chunks
    tuple_index = 0
    chunk_amount = duckdb_result_chunk_count(result)
    do chunk_index = 0, chunk_amount - 1
      print*, chunk_index
      chunk = duckdb_result_get_chunk(result, chunk_index)

      ! Get Vector and Validity
      vector = duckdb_data_chunk_get_vector(chunk, 0)
      vector_ptr = duckdb_vector_get_validity(vector)
      call c_f_pointer(vector_ptr, vector_validity)

      ! FIXME: Original cpp code reads
      ! auto string_data = (duckdb_string_t *)duckdb_vector_get_data(vector);
      string_data = duckdb_vector_get_data(vector)

      ! Get Tuples in Chunk
      tuples_in_chunk = duckdb_data_chunk_get_size(chunk)
      print '(B0)', vector_validity
      do i = 0, tuples_in_chunk - 1
        print*, i
        if ( .not. duckdb_validity_row_is_valid(vector_validity, i)) then
          ! The query produces data formatted like below. Every 42 rows there is a NULL.
          ! Every letter is repeated from 4 to 16 increasing by 1 on each row. 
          ! The example below is run with range(3) as an input. In the test query we have range(5000)
          ! duckdb> select case when i != 0 and i % 42 = 0 then NULL else repeat(chr((65 + (i % 26))::INTEGER), (4 + (i % 12))) end from range(3) tbl(i);
          !   ┌───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
          !   │ CASE  WHEN (((i != 0) AND ((i % 42) = 0))) THEN (NULL) ELSE repeat(chr(CAST((65 + (i % 26)) AS INTEGER)), (4 + (i % 12))) END │
          !   ╞═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════╡
          !   │ AAAA                                                                                                                          │
          !   │ BBBBB                                                                                                                         │
          !   │ CCCCCC                                                                                                                        │
          !   └───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
          print*, i," is invalid."
          
          call check(error, (tuple_index /= 0) .and. (mod(tuple_index, 42) == 0), &
            "Invalid NULL entry at tuple index")
          if (allocated(error)) return
          cycle
        end if

      !   ! Calculate Expected Length and Character
      !   expected_length = mod(tuple_index, 12) + 4
      !   expected_character = char(mod(tuple_index, 26) + 65)

      !   ! Get Tuple and Length
      !   length = duckdb_string_t_value_inlined_length(string_data(i))
      !   if (length /= expected_length) then
      !     print *, "Invalid length at tuple index", tuple_index
      !     return
      !   end if

      !   ! Check Tuple Data
      !   if (duckdb_string_is_inlined(string_data(i))) then
      !     ! Data is inlined
      !     do string_index = 1, length
      !       if (string_data(i)%value%inlined(string_index) /= expected_character) then
      !         print *, "Invalid character at tuple index", tuple_index
      !         return
      !       end if
      !     end do
      !   else
      !     ! Data has separate allocation
      !     do string_index = 1, length
      !       if (string_data(i)%value%pointer%ptr(string_index) /= expected_character) then
      !         print *, "Invalid character at tuple index", tuple_index
      !         return
      !       end if
      !     end do
      !   end if

        tuple_index = tuple_index + 1
      end do

      ! Destroy Chunk
      call duckdb_destroy_data_chunk(chunk)
    end do

    ! Destroy Result
    call duckdb_destroy_result(result)

    ! Disconnect from Database
    call duckdb_disconnect(conn)

    ! Close Database
    call duckdb_close(db)

  end subroutine test_data_chunk_varchar_result_fetch

end module test_data_chunk
