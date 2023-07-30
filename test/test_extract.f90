module test_extract
  use, intrinsic :: iso_c_binding, only: c_associated
  use constants
  use duckdb
  use testdrive, only: new_unittest, unittest_type, error_type, check, skip_test
  implicit none
  private
  public :: collect_extract
contains
  subroutine collect_extract(testsuite)
    type(unittest_type), allocatable, intent(out) :: testsuite(:)
    testsuite = [                                                               &
      new_unittest("extract-statements-test", test_extract_statements)          &
    ]
  end subroutine collect_extract

  subroutine test_extract_statements(error)
    type(error_type), allocatable, intent(out) :: error
    type(duckdb_database) :: database
    type(duckdb_connection) :: connection
    type(duckdb_extracted_statements) :: stmts = duckdb_extracted_statements()
    type(duckdb_prepared_statement) :: stmt
    integer(kind(duckdb_state)) :: status
    integer :: size, i
    character(len=:), allocatable :: errmsg
    type(duckdb_prepared_statement) :: prepared = duckdb_prepared_statement()
    type(duckdb_result) :: res

    call check(error, duckdb_open("", database) == duckdbsuccess, "error opening db.")
    if (allocated(error)) return
    call check(error, duckdb_connect(database, connection) == duckdbsuccess, "error connecting.")
    if (allocated(error)) return

    size = duckdb_extract_statements(connection,                                &
      "CREATE TABLE tbl (col INT); INSERT INTO tbl VALUES (1), (2), (3), (4);   &
      &SELECT COUNT(col) FROM tbl WHERE col > $1",                              &
      stmts)
    call check(error, size == 3, "error in size of extracted statements.")
    if (allocated(error)) return
    call check(error, c_associated(stmts%extrac), "unallocated extracted statements.")
    if (allocated(error)) return
    do i = 0, size - 2  
      status = duckdb_prepare_extracted_statement(connection, stmts, int(i, kind=int64), prepared)
      call check(error, status == duckdbsuccess, "error in extract prepare statements.")
      if (allocated(error)) return
      status = duckdb_execute_prepared(prepared, res)
      call check(error, status == duckdbsuccess, "error in execute prepared statements.")
      if (allocated(error)) return

      call duckdb_destroy_prepare(prepared)
      call duckdb_destroy_result(res)
    enddo

    stmt = duckdb_prepared_statement()
    status = duckdb_prepare_extracted_statement(connection, stmts, int(size - 1, kind=int64), stmt)
    call check(error, status == duckdbsuccess, "error in last extract prepared.")
    if (allocated(error)) return
    status = duckdb_bind_int32(stmt, 1, 1)
    call check(error, status == duckdbsuccess, "error in last bind int32.")
    if (allocated(error)) return
    status = duckdb_execute_prepared(stmt, res)
    call check(error, status == duckdbsuccess, "error in last execute prepared.")
    if (allocated(error)) return
    call check(error, duckdb_value_int64(res, 0, 0) == 3, "error in retriving value.")
    if (allocated(error)) return
    call duckdb_destroy_prepare(stmt)
    call duckdb_destroy_result(res)
    call duckdb_destroy_extracted(stmts)

    ! test empty statement is not an error
    size = duckdb_extract_statements(connection, "", stmts)
    call check(error, size == 0, "error getting size from extract statement.")
    if (allocated(error)) return
    errmsg = duckdb_extract_statements_error(stmts)
    call check(error, errmsg == "", "error in extracting error message.")
    if (allocated(error)) return
    call duckdb_destroy_extracted(stmts)

    ! test incorrect statement cannot be extracted
    size = duckdb_extract_statements(connection, "This is not valid SQL", stmts)
    call check(error, size == 0, "error getting size from invalid statement.")
    if (allocated(error)) return
    errmsg = duckdb_extract_statements_error(stmts)
    call check(error, errmsg /= "", "error in extracting error message 2.")
    call duckdb_destroy_extracted(stmts)

    ! test out of bounds
    size = duckdb_extract_statements(connection, "SELECT CAST($1 AS BIGINT)", stmts)
    call check(error, size == 1, "error getting size from valid statement.")
    if (allocated(error)) return
    status = duckdb_prepare_extracted_statement(connection, stmts, 2_int64, prepared)
    call check(error, status == DuckDBError, "no error in last prepare extracted stmt.")
    if (allocated(error)) return
    call duckdb_destroy_extracted(stmts)
  end subroutine test_extract_statements
end module test_extract
