module test_pending
  use, intrinsic :: iso_c_binding, only: c_associated
  use constants
  use duckdb
  use testdrive, only: new_unittest, unittest_type, error_type, check, skip_test
  implicit none
  private
  public :: collect_pending
contains
  subroutine collect_pending(testsuite)
    type(unittest_type), allocatable, intent(out) :: testsuite(:)
    testsuite = [                                                               &
      new_unittest("pending-statements-test", test_pending_statements)          &
    ]
  end subroutine collect_pending

  subroutine test_pending_statements(error)
    type(error_type), allocatable, intent(out) :: error
    type(duckdb_database) :: database
    type(duckdb_connection) :: connection
    type(duckdb_prepared_statement) :: stmt
    integer(kind(duckdb_state)) :: status, success
    integer :: size, i
    type(duckdb_pending_result) :: res
    type(duckdb_result) :: res2
    

    call check(error, duckdb_open("", database) == duckdbsuccess, "error opening db.")
    if (allocated(error)) return
    call check(error, duckdb_connect(database, connection) == duckdbsuccess, "error connecting.")
    if (allocated(error)) return
    status = duckdb_prepare(connection, "SELECT SUM(i) FROM range(1000000) tbl(i)", stmt)
    call check(error, status == duckdbsuccess, "error preparing query.")
    if (allocated(error)) return

    status = duckdb_pending_prepared(stmt, res)
    call check(error, status == duckdbsuccess, "error with pending prepared.")
    if (allocated(error)) return

    do
      call check(error, c_associated(stmt%prep), "pending statement not associated.")
      if (allocated(error)) return
      status = duckdb_pending_execute_task(res)
      call check(error, status /= duckdb_pending_error_state, "execute task error.")
      if (allocated(error)) return
      if (status == DUCKDB_PENDING_RESULT_READY) exit
    enddo      

    success = duckdb_execute_pending(res, res2)
    call check(error, success == duckdbsuccess, "error in execute pending.")
    if (allocated(error)) return

    call check(error, duckdb_value_int64(res2, 0, 0) == 499999500000_int64, "error in matching result pending.")
    if (allocated(error)) return

    call duckdb_destroy_result(res2)
    call duckdb_destroy_prepare(stmt)
    call duckdb_destroy_pending(res)
  end subroutine test_pending_statements
end module test_pending
