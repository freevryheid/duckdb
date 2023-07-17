module test_complex_types
  use, intrinsic :: iso_c_binding
  use duckdb
  use constants
  use strings, only: string_t
  use testdrive, only: new_unittest, unittest_type, error_type, check, skip_test
  implicit none
  private
  public :: collect_complex_types
contains

  subroutine collect_complex_types(testsuite)

    type(unittest_type), allocatable, intent(out) :: testsuite(:)

    testsuite = [                                                       &
      new_unittest("decimal-types-test", test_decimal_types),           &
      new_unittest("enum-types-test", test_enum_types),                 &
      new_unittest("list-types-test", test_list_types)                  &
    ]

  end subroutine collect_complex_types

  subroutine test_decimal_types(error)
    type(error_type), allocatable, intent(out) :: error
    type(duckdb_database) :: db
    type(duckdb_connection) :: con
    type(duckdb_result) :: result = duckdb_result()
    integer :: i, j, nc, nr
    type(duckdb_data_chunk) :: chunk
    integer(kind(duckdb_type)), allocatable :: types(:), internal_types(:)
    integer(kind=int8), allocatable :: scales(:), widths(:)
    type(duckdb_logical_type) :: logical_type

    ! Open data in in-memory mode
    call check(error, duckdb_open("", db) == duckdbsuccess)
    if (allocated(error)) return

    call check(error, duckdb_connect(db, con) == duckdbsuccess)
    if (allocated(error)) return

    ! create a table from reading a parquet file
    call check(error, duckdb_query( &
      con, &
      "SELECT 1.0::DECIMAL(4,1), 2.0::DECIMAL(9,2), 3.0::DECIMAL(18,3), 4.0::DEC&
      &IMAL(38,4), 5::INTEGER", &
      result) == duckdbsuccess)
    if (allocated(error)) return

    nc = duckdb_column_count(result)
    call check(error, nc == 5, "wrong number of columns.")
    if (allocated(error)) return

    call check(error, duckdb_result_error(result) == "", "error message.")
    if (allocated(error)) return

    if (duckdb_vector_size() < 60) then
      print*, "vector size:",duckdb_vector_size()
      return
    endif

    ! Fetch the first chunk
    call check(error, duckdb_result_chunk_count(result) == 1, "wrong chunk count.")
    if (allocated(error)) return

    chunk = duckdb_result_get_chunk(result, 0)
    call check(error, c_associated(chunk%dtck), "chunk not associated.")
    if (allocated(error)) return

    widths = [4_int8, 9_int8, 18_int8, 38_int8, 0_int8]
    scales = [1_int8, 2_int8, 3_int8, 4_int8, 0_int8]

    types = [duckdb_type_decimal, duckdb_type_decimal, duckdb_type_decimal, &
      duckdb_type_decimal, duckdb_type_integer]
    internal_types = [duckdb_type_smallint, duckdb_type_integer, duckdb_type_bigint, &
      duckdb_type_hugeint, duckdb_type_invalid]
    
    do i = 0, duckdb_column_count(result)-1
      logical_type = duckdb_vector_get_column_type(duckdb_data_chunk_get_vector(chunk, i))
      call check(error, c_associated(logical_type%lglt), "type not associated.")
      if (allocated(error)) return
      call check(error, duckdb_get_type_id(logical_type) == types(i+1), "wrong type.")
      if (allocated(error)) return
      call check(error, duckdb_decimal_width(logical_type) == widths(i+1), "wrong width.")
      if (allocated(error)) return
      call check(error, duckdb_decimal_scale(logical_type) == scales(i+1), "wrong scale.")
      if (allocated(error)) return
      call check(error, duckdb_decimal_internal_type(logical_type) == internal_types(i+1), &
        "wrong internal type.")
      if (allocated(error)) return

      call duckdb_destroy_logical_type(logical_type)
    enddo

    call check(error, duckdb_decimal_width(logical_type) == 0, "unassigned: wrong width.")
    if (allocated(error)) return
    call check(error, duckdb_decimal_scale(logical_type) == 0, "unassigned: wrong scale.")
    if (allocated(error)) return
    call check(error, duckdb_decimal_internal_type(logical_type) == duckdb_type_invalid, &
      "unassigned: wrong internal type.")

    call duckdb_destroy_result(result)
    call duckdb_disconnect(con)
    call duckdb_close(db)
  end subroutine test_decimal_types

  subroutine test_enum_types(error)
    type(error_type), allocatable, intent(out) :: error
    type(duckdb_database) :: db
    type(duckdb_connection) :: con
    type(duckdb_result) :: result = duckdb_result()
    integer :: i, j, nc, nr
    type(duckdb_data_chunk) :: chunk
    integer(kind(duckdb_type)), allocatable :: types(:), internal_types(:)
    integer(kind=int32), allocatable :: dictionary_sizes(:)
    type(string_t), allocatable :: dictionary_strings(:)
    type(duckdb_logical_type) :: logical_type
    character(len=:), allocatable :: val

    ! Open data in in-memory mode
    call check(error, duckdb_open("", db) == duckdbsuccess, "failed to open db.")
    if (allocated(error)) return

    call check(error, duckdb_connect(db, con) == duckdbsuccess, "failed to open connection.")
    if (allocated(error)) return

    if (duckdb_vector_size() < 60) then
      print*, "vector size:",duckdb_vector_size()
      return
    endif

    ! create a table from reading a parquet file
    call check(error, duckdb_query( &
      con, &
      "select small_enum, medium_enum, large_enum, int from test_all_types();", &
      result) == duckdbsuccess, "failed query.")    
    if (allocated(error)) return

    nc = duckdb_column_count(result)
    call check(error, nc == 4, "wrong number of columns.")
    if (allocated(error)) return

    call check(error, duckdb_result_error(result) == "", "error message.")
    if (allocated(error)) return

    ! Fetch the first chunk
    call check(error, duckdb_result_chunk_count(result) == 1, "wrong chunk count.")
    if (allocated(error)) return

    chunk = duckdb_result_get_chunk(result, 0)
    call check(error, c_associated(chunk%dtck), "chunk not associated.")
    if (allocated(error)) return

    dictionary_sizes = [2, 300, 70000, 0]
    dictionary_strings = [string_t("DUCK_DUCK_ENUM"), string_t("enum_0"), &
      string_t("enum_0"), string_t("")]

    types = [DUCKDB_TYPE_ENUM, DUCKDB_TYPE_ENUM, DUCKDB_TYPE_ENUM, &
      DUCKDB_TYPE_INTEGER]
    internal_types = [DUCKDB_TYPE_UTINYINT, DUCKDB_TYPE_USMALLINT, &
      DUCKDB_TYPE_UINTEGER, DUCKDB_TYPE_INVALID]
    
    do i = 0, duckdb_column_count(result)-1
      logical_type = duckdb_vector_get_column_type(duckdb_data_chunk_get_vector(chunk, i))
      call check(error, c_associated(logical_type%lglt), "type not associated.")
      if (allocated(error)) return
      call check(error, duckdb_get_type_id(logical_type) == types(i+1), &
        "wrong type.")
      if (allocated(error)) return
      call check(error, &
        duckdb_enum_internal_type(logical_type) == internal_types(i+1), &
        "wrong internal type.")
      if (allocated(error)) return
      call check(error, &
        duckdb_enum_dictionary_size(logical_type) == dictionary_sizes(i+1), &
        "wrong dictionary size.")
      if (allocated(error)) return

      val = duckdb_enum_dictionary_value(logical_type, 0)   
      call check(error, &
        string_t(val) == dictionary_strings(i+1), &
        "wrong dictionary value.")
      if (allocated(error)) return

      call duckdb_destroy_logical_type(logical_type)
    enddo

    call check(error, &
      duckdb_enum_internal_type(logical_type) == DUCKDB_TYPE_INVALID, &
      "unassigned: wrong internal type.")
    if (allocated(error)) return
    call check(error, &
      duckdb_enum_dictionary_size(logical_type) == 0, &
      "unassigned: wrong dictionary size.")
    if (allocated(error)) return
    call check(error, duckdb_enum_dictionary_value(logical_type, 0) == "", &
      "unassigned: wrong dictionary value.")

    call duckdb_destroy_result(result)
    call duckdb_disconnect(con)
    call duckdb_close(db)
  end subroutine test_enum_types

  subroutine test_list_types(error)
    type(error_type), allocatable, intent(out) :: error
    type(duckdb_database) :: db
    type(duckdb_connection) :: con
    type(duckdb_result) :: result = duckdb_result()
    integer :: i, j, nc, nr
    type(duckdb_data_chunk) :: chunk
    integer(kind(duckdb_type)), allocatable :: types(:)
    integer(kind(duckdb_type)), allocatable :: child_types_1(:)
    integer(kind(duckdb_type)), allocatable :: child_types_2(:)
    type(duckdb_logical_type) :: logical_type, child_type1, child_type2
    character(len=:), allocatable :: val

    ! Open data in in-memory mode
    call check(error, duckdb_open("", db) == duckdbsuccess, "failed to open db.")
    if (allocated(error)) return

    call check(error, duckdb_connect(db, con) == duckdbsuccess, "failed to open connection.")
    if (allocated(error)) return

    ! create a table from reading a parquet file
    call check(error, duckdb_query( &
      con, &
      "select [1, 2, 3] l, ['hello', 'world'] s, [[1, 2, 3], [4, 5]] nested, 3::int", &
      result) == duckdbsuccess, "failed query.")    
    if (allocated(error)) return

    nc = duckdb_column_count(result)
    call check(error, nc == 4, "wrong number of columns.")
    if (allocated(error)) return

    call check(error, duckdb_result_error(result) == "", "error message.")
    if (allocated(error)) return

    ! Fetch the first chunk
    call check(error, duckdb_result_chunk_count(result) == 1, "wrong chunk count.")
    if (allocated(error)) return

    chunk = duckdb_result_get_chunk(result, 0)
    call check(error, c_associated(chunk%dtck), "chunk not associated.")
    if (allocated(error)) return

    types = [DUCKDB_TYPE_LIST, DUCKDB_TYPE_LIST, DUCKDB_TYPE_LIST, &
      DUCKDB_TYPE_INTEGER]
    child_types_1 = [DUCKDB_TYPE_INTEGER, DUCKDB_TYPE_VARCHAR, DUCKDB_TYPE_LIST, &
      DUCKDB_TYPE_INVALID]
    child_types_2 = [DUCKDB_TYPE_INVALID, DUCKDB_TYPE_INVALID, DUCKDB_TYPE_INTEGER, &
    DUCKDB_TYPE_INVALID]
    
    do i = 0, duckdb_column_count(result)-1
      logical_type = duckdb_vector_get_column_type(duckdb_data_chunk_get_vector(chunk, i))
      call check(error, c_associated(logical_type%lglt), "type not associated.")
      if (allocated(error)) return
      call check(error, duckdb_get_type_id(logical_type) == types(i+1), &
        "wrong type.")
      if (allocated(error)) return
      child_type1 = duckdb_list_type_child_type(logical_type)
      child_type2 = duckdb_list_type_child_type(child_type1)

      call check(error, &
        duckdb_get_type_id(child_type1) == child_types_1(i+1), &
        "wrong child type 1.")
      if (allocated(error)) return
      call check(error, &
        duckdb_get_type_id(child_type2) == child_types_2(i+1), &
        "wrong child type 2.")
      if (allocated(error)) return

      call duckdb_destroy_logical_type(logical_type)
      call duckdb_destroy_logical_type(child_type1)
      call duckdb_destroy_logical_type(child_type2)
    enddo

    child_type1 = duckdb_list_type_child_type(logical_type)
    call check(error, &
      .not. c_associated(child_type1%lglt), &
      "unassigned: wrong child type.")
    if (allocated(error)) return

    call duckdb_destroy_result(result)
    call duckdb_disconnect(con)
    call duckdb_close(db)
  end subroutine test_list_types

end module test_complex_types
