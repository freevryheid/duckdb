module test_starting_database
  use, intrinsic :: iso_c_binding
  use duckdb
  use testdrive, only: new_unittest, unittest_type, error_type, check, skip_test
  implicit none
  private
  public :: collect_starting_database
contains

  subroutine collect_starting_database(testsuite)
    type(unittest_type), allocatable, intent(out) :: testsuite(:)
    testsuite = [ &
                new_unittest("simple-startup", test_simple_startup), &
                new_unittest("multiple-startup", test_multiple_startup)]
  end subroutine collect_starting_database

  subroutine test_simple_startup(error)
    type(error_type), allocatable, intent(out) :: error
    type(duckdb_database) :: database
    type(duckdb_connection) :: connection
    call check(error, duckdb_open(c_null_ptr, database) == duckdbsuccess)
    if (allocated(error)) return
    call check(error, duckdb_connect(database, connection) == duckdbsuccess)
    if (allocated(error)) return
    call duckdb_disconnect(connection)
    call duckdb_close(database)
  end subroutine test_simple_startup

  subroutine test_multiple_startup(error)
    type(error_type), allocatable, intent(out) :: error
    type(duckdb_database) :: database(10)
    type(duckdb_connection) :: connection(100)
    integer :: i, j
    do i = 1, 10
      call check(error, duckdb_open(c_null_ptr, database(i)) == duckdbsuccess)
      if (allocated(error)) return
      do j = 1, 10
        call check(error, duckdb_connect(database(i), connection((i - 1)*10 + j)) == duckdbsuccess)
        if (allocated(error)) return
      end do
    end do
    do i = 1, 10
      do j = 1, 10
        call duckdb_disconnect(connection((i - 1)*10 + j))
      end do
      call duckdb_close(database(i))
    end do
  end subroutine test_multiple_startup

end module test_starting_database
