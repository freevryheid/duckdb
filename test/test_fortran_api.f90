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
                new_unittest("basic-api-test", test_basic) &
                ! new_unittest("multiple-startup", test_multiple_startup) &
                ]
  end subroutine collect_fortran_api

  subroutine test_basic(error)
    type(error_type), allocatable, intent(out) :: error

    type(c_ptr) :: database, connection, chunk
    type(duckdb_result), pointer :: result

    ! Open data in in-memory mode
    call check(error, duckdb_open(c_null_ptr, database) == duckdbsuccess)
    if (allocated(error)) return

    call check(error, duckdb_connect(database, connection) == duckdbsuccess)
    if (allocated(error)) return

    ! select scalar value
    call check(error, &
      duckdb_query(connection, "SELECT CAST(42 AS BIGINT)", result) == duckdbsuccess) 
    if (allocated(error)) return

    call check(error, duckdb_column_type(result, 0) == duckdb_type_bigint)
    if (allocated(error)) return

    ! call check(error, duckdb_value_int64(result, 0, 0) == 42)
    ! if (allocated(error)) return

    call check(error, duckdb_column_count(result) == 1)
    if (allocated(error)) return

    call check(error, duckdb_row_count(result) == 1)
    if (allocated(error)) return

    ! cannot fetch data chunk after using the value API
    chunk = duckdb_result_get_chunk(result, 0)
    call check(error, .not. c_associated(chunk))
    if (allocated(error)) return

    ! call duckdb_destroy_data_chunk(chunk)
    call duckdb_destroy_result(result)
    call duckdb_disconnect(connection)
    call duckdb_close(database)
  end subroutine test_basic

end module test_fortran_api