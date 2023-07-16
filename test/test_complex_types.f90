module test_complex_types
  use, intrinsic :: iso_c_binding
  use duckdb
  use constants
  use testdrive, only: new_unittest, unittest_type, error_type, check, skip_test
  implicit none
  private
  public :: collect_complex_types
contains

subroutine collect_complex_types(testsuite)

  type(unittest_type), allocatable, intent(out) :: testsuite(:)

  testsuite = [                                                       &
    new_unittest("decimal-types-test", test_decimal_types)            &
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
end module test_complex_types
