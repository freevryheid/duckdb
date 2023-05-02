module test_appender

  use, intrinsic :: iso_c_binding
  use duckdb
  use constants
  use testdrive, only: new_unittest, unittest_type, error_type, check, skip_test

  implicit none

  private
  public :: collect_appender

contains

  subroutine collect_appender(testsuite)

    type(unittest_type), allocatable, intent(out) :: testsuite(:)

    testsuite = [ &
                new_unittest("appender-statements-test", test_appender_statements) & !, &
                ! new_unittest("append-timestamp-test", test_append_timestamp) &
                ]

  end subroutine collect_appender

  subroutine test_appender_statements(error)

    type(error_type), allocatable, intent(out) :: error
    type(duckdb_database) :: db
    type(duckdb_connection) :: con
    type(duckdb_result) :: result = duckdb_result()

    type(duckdb_appender) :: appender, a
    integer(kind=kind(duckdb_state)) :: status

    ! Open db in in-memory mode
    call check(error, duckdb_open(c_null_ptr, db) == duckdbsuccess, "Could not open db.")
    if (allocated(error)) return
    call check(error, duckdb_connect(db, con) == duckdbsuccess, "Could not start connection.")
    if (allocated(error)) return

    call check(error, duckdb_query(con, &
                                   "CREATE TABLE test (i INTEGER, d double, s string)", &
                                   result) /= duckdberror, "Could not run query.")
    if (allocated(error)) return

    status = duckdb_appender_create(con, "", "nonexistant-table", appender)
    call check(error, status == duckdberror, "Appender did not return error.")
    if (allocated(error)) return 

    call check(error, c_associated(appender%appn), "Appender is not associated.")
    if (allocated(error)) return

    call check(error, duckdb_appender_error(appender) /= "NULL", "Appender error message is not empty.")
    if (allocated(error)) return

    call check(error, duckdb_appender_destroy(appender) == duckdbsuccess, "Appender not destroyed successfully.")
    if (allocated(error)) return

    call check(error, duckdb_appender_destroy(a) == duckdberror, "Destroy unallocated appender does not error.")
    if (allocated(error)) return

    !! FIXME: fortran does not let us pass a nullptr here as in the c++ test. 
    ! status = duckdb_appender_create(con, "", "test", c_null_ptr)
    ! call check(error, status == duckdberror, "Appender did not return error.")
    ! if (allocated(error)) return 
    ! status = duckdb_appender_create(tester.connection, nullptr, "test", nullptr);
    ! REQUIRE(status == DuckDBError);

    status = duckdb_appender_create(con, "", "test", appender)
    call check(error, status == duckdbsuccess, "Appender creation error.")
    if (allocated(error)) return 

    call check(error, duckdb_appender_error(appender) == "NULL", "Appender has error message.")
    if (allocated(error)) return 

    status = duckdb_appender_begin_row(appender)
    call check(error, status == duckdbsuccess, "duckdb_appender_begin_row error.")
    if (allocated(error)) return 

    status = duckdb_append_int32(appender, 42)
    call check(error, status == duckdbsuccess, "duckdb_append_int32 error.")
    if (allocated(error)) return 

    status = duckdb_append_double(appender, 4.2_real64)
    call check(error, status == duckdbsuccess, "duckdb_append_double error.")
    if (allocated(error)) return 

    status = duckdb_append_varchar(appender, "Hello, World")
    call check(error, status == duckdbsuccess, "duckdb_append_varchar error.")
    if (allocated(error)) return 

    !! out of columns. Should give error.
    status = duckdb_append_int32(appender, 42)
    call check(error, status == duckdberror, "duckdb_append_int32 does not error.")
    if (allocated(error)) return 

    status = duckdb_appender_end_row(appender)
    call check(error, status == duckdbsuccess, "duckdb_appender_end_row error.")
    if (allocated(error)) return 

    status = duckdb_appender_flush(appender)
    call check(error, status == duckdbsuccess, "duckdb_appender_flush error.")
    if (allocated(error)) return 

    status = duckdb_appender_begin_row(appender)
    call check(error, status == duckdbsuccess, "duckdb_appender_begin_row 2 error.")
    if (allocated(error)) return 

    status = duckdb_append_int32(appender, 42)
    call check(error, status == duckdbsuccess, "duckdb_append_int32 2 error.")
    if (allocated(error)) return 
  
    status = duckdb_append_double(appender, 4.2_real64)
    call check(error, status == duckdbsuccess, "duckdb_append_double 2 error.")
    if (allocated(error)) return 

    ! not enough columns here
    status = duckdb_appender_end_row(appender)
    call check(error, status == duckdberror, "Can end row despite not enough columns.")
    if (allocated(error)) return 

    call check(error, duckdb_appender_error(appender) /= "NULL")
    if (allocated(error)) return
  end subroutine test_appender_statements
end module test_appender
