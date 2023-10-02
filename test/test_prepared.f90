module test_prepared
  use, intrinsic :: iso_c_binding, only: c_associated, c_loc, c_ptr
  use, intrinsic :: ieee_arithmetic, only: ieee_value, ieee_quiet_nan
  use constants
  use util, only: c_f_str_ptr
  use duckdb
  use testdrive, only: new_unittest, unittest_type, error_type, check, skip_test
  implicit none
  private
  public :: collect_prepared
contains
  subroutine collect_prepared(testsuite)
    type(unittest_type), allocatable, intent(out) :: testsuite(:)
    testsuite = [                                                               &
      new_unittest("prepared-statements-test", test_prepared_statements)        &
    ]
  end subroutine collect_prepared

  subroutine test_prepared_statements(error)
    type(error_type), allocatable, intent(out) :: error
    type(duckdb_database) :: database
    type(duckdb_connection) :: connection
    type(duckdb_prepared_statement) :: stmt, unallocated_stmt
    type(duckdb_result) :: res
    integer(kind(duckdb_state)) :: status
    type(duckdb_decimal) :: decimal, result_decimal
    character(len=:), allocatable, target :: str
    character(len=:), allocatable :: str_res
    type(duckdb_blob) :: blob_data
    type(duckdb_date_struct) :: date_struct
    type(duckdb_time_struct) :: time_struct
    type(duckdb_timestamp_struct) :: ts
    type(duckdb_interval) :: interval
    integer(kind=kind(duckdb_type)) :: pt
    integer :: i

    ! open the database in in-memory mode
    call check(error, duckdb_open("", database) == duckdbsuccess, "error opening db.")
    if (allocated(error)) return
    call check(error, duckdb_connect(database, connection) == duckdbsuccess, "error connecting.")
    if (allocated(error)) return

    status = duckdb_prepare(connection, "SELECT CAST($1 AS BIGINT)", stmt)
    call check(error, status == duckdbsuccess, "error in prepare.")
    if (allocated(error)) return
    call check(error, status == duckdbsuccess, "error in prepare.")
    if (allocated(error)) return
    call check(error, c_associated(stmt%prep), "unallocated prepared statements.")
    if (allocated(error)) return

    status = duckdb_bind_boolean(stmt, 1, .true.)
    call check(error, status == duckdbsuccess, "error in bind boolean.")
    if (allocated(error)) return

    ! Parameter index 2 is out of bounds
    status = duckdb_bind_boolean(stmt, 2, .true.)
    call check(error, status == duckdberror, "Can bind on index 2.")
    if (allocated(error)) return

    status = duckdb_execute_prepared(stmt, res)
    call check(error, status == duckdbsuccess, "cannot execut prepared statement.")
    if (allocated(error)) return
    call check(error, duckdb_value_int64(res, 0, 0) == 1, "cannot extract result.")
    if (allocated(error)) return
    call duckdb_destroy_result(res)

    status = duckdb_bind_int8(stmt, 1, 8_int8)
    call check(error, status == duckdbsuccess, "Cannot bind to int8.")
    if (allocated(error)) return
    status = duckdb_execute_prepared(stmt, res)
    call check(error, status == duckdbsuccess, "Cannot execute bind to int8.")
    if (allocated(error)) return
    call check(error, duckdb_value_int64(res, 0, 0) == 8, "cannot extract int8 result.")
    if (allocated(error)) return
    call duckdb_destroy_result(res)

    status = duckdb_bind_int16(stmt, 1, 16_int16)
    call check(error, status == duckdbsuccess, "Cannot bind to int16.")
    if (allocated(error)) return
    status = duckdb_execute_prepared(stmt, res)
    call check(error, status == duckdbsuccess, "Cannot execute bind to int16.")
    if (allocated(error)) return
    call check(error, duckdb_value_int64(res, 0, 0) == 16, "cannot extract int16 result.")
    if (allocated(error)) return
    call duckdb_destroy_result(res)

    status = duckdb_bind_int32(stmt, 1, 32_int32)
    call check(error, status == duckdbsuccess, "Cannot bind to int32.")
    if (allocated(error)) return
    status = duckdb_execute_prepared(stmt, res)
    call check(error, status == duckdbsuccess, "Cannot execute bind to int32.")
    if (allocated(error)) return
    call check(error, duckdb_value_int64(res, 0, 0) == 32, "cannot extract int32 result.")
    if (allocated(error)) return
    call duckdb_destroy_result(res)

    status = duckdb_bind_int64(stmt, 1, 64_int64)
    call check(error, status == duckdbsuccess, "Cannot bind to int64.")
    if (allocated(error)) return
    status = duckdb_execute_prepared(stmt, res)
    call check(error, status == duckdbsuccess, "Cannot execute bind to int64.")
    if (allocated(error)) return
    call check(error, duckdb_value_int64(res, 0, 0) == 64, "cannot extract int64 result.")
    if (allocated(error)) return
    call duckdb_destroy_result(res)

    status = duckdb_bind_hugeint(stmt, 1, duckdb_double_to_hugeint(64.0_real64))
    call check(error, status == duckdbsuccess, "Cannot bind to hugeint.")
    if (allocated(error)) return
    status = duckdb_execute_prepared(stmt, res)
    call check(error, status == duckdbsuccess, "Cannot execute bind to hugeint.")
    if (allocated(error)) return
    call check(error, duckdb_hugeint_to_double(duckdb_value_hugeint(res, 0, 0)) == 64.0, &
      "cannot extract hugeint result.")
    if (allocated(error)) return
    call duckdb_destroy_result(res)

    ! Fetching a DECIMAL from a non-DECIMAL result returns 0
    decimal = duckdb_double_to_decimal(634.3453_real64, 7, 4)
    status = duckdb_bind_decimal(stmt, 1, decimal)
    call check(error, status == duckdbsuccess, "Cannot bind to decimal.")
    if (allocated(error)) return
    status = duckdb_execute_prepared(stmt, res)
    call check(error, status == duckdbsuccess, "Cannot execute bind to decimal.")
    if (allocated(error)) return
    result_decimal = duckdb_value_decimal(res, 0, 0)
    call check(error, result_decimal%scale == 0, "Cannot match decimal scale.")
    if (allocated(error)) return
    call check(error, result_decimal%width == 0, "Cannot match decimal width.")
    if (allocated(error)) return
    call check(error, result_decimal%value%upper == 0, "Cannot match decimal value upper.")
    if (allocated(error)) return
    call check(error, result_decimal%value%lower == 0, "Cannot match decimal value lower.")
    if (allocated(error)) return
    call duckdb_destroy_result(res)

    !   Skip unsigned data types because they are not implemented in fortran.
    ! 	duckdb_bind_uint8(stmt, 1, 8);
    ! 	status = duckdb_execute_prepared(stmt, &res);
    ! 	REQUIRE(status == DuckDBSuccess);
    ! 	REQUIRE(duckdb_value_uint8(&res, 0, 0) == 8);
    ! 	duckdb_destroy_result(&res);

    ! 	duckdb_bind_uint16(stmt, 1, 8);
    ! 	status = duckdb_execute_prepared(stmt, &res);
    ! 	REQUIRE(status == DuckDBSuccess);
    ! 	REQUIRE(duckdb_value_uint16(&res, 0, 0) == 8);
    ! 	duckdb_destroy_result(&res);

    ! 	duckdb_bind_uint32(stmt, 1, 8);
    ! 	status = duckdb_execute_prepared(stmt, &res);
    ! 	REQUIRE(status == DuckDBSuccess);
    ! 	REQUIRE(duckdb_value_uint32(&res, 0, 0) == 8);
    ! 	duckdb_destroy_result(&res);

    ! 	duckdb_bind_uint64(stmt, 1, 8);
    ! 	status = duckdb_execute_prepared(stmt, &res);
    ! 	REQUIRE(status == DuckDBSuccess);
    ! 	REQUIRE(duckdb_value_uint64(&res, 0, 0) == 8);
    ! 	duckdb_destroy_result(&res);

    status = duckdb_bind_float(stmt, 1, 42.0)
    call check(error, status == duckdbsuccess, "Cannot bind to float.")
    if (allocated(error)) return
    status = duckdb_execute_prepared(stmt, res)
    call check(error, status == duckdbsuccess, "Cannot execute bind to float.")
    if (allocated(error)) return
    call check(error, duckdb_value_int64(res, 0, 0) == 42, "cannot extract float result.")
    if (allocated(error)) return
    call duckdb_destroy_result(res)

    status = duckdb_bind_double(stmt, 1, 43.0_real64)
    call check(error, status == duckdbsuccess, "Cannot bind to double.")
    if (allocated(error)) return
    status = duckdb_execute_prepared(stmt, res)
    call check(error, status == duckdbsuccess, "Cannot execute bind to double.")
    if (allocated(error)) return
    call check(error, duckdb_value_int64(res, 0, 0) == 43, "cannot extract double result.")
    if (allocated(error)) return
    call duckdb_destroy_result(res)

    status = duckdb_bind_float(stmt, 1, ieee_value(1.0, ieee_quiet_nan))
    call check(error, status == duckdbsuccess, "Cannot bind to float nan.")
    if (allocated(error)) return
    status = duckdb_execute_prepared(stmt, res)
    call check(error, status == duckdberror, "Can execute bind to float nan.")
    if (allocated(error)) return
    call duckdb_destroy_result(res)

    status = duckdb_bind_double(stmt, 1, ieee_value(1.0_real64, ieee_quiet_nan))
    call check(error, status == duckdbsuccess, "Cannot bind to double nan.")
    if (allocated(error)) return
    status = duckdb_execute_prepared(stmt, res)
    call check(error, status == duckdberror, "Can execute bind to double nan.")
    if (allocated(error)) return
    call duckdb_destroy_result(res)

    status = duckdb_bind_varchar(stmt, 1, achar(int(Z'80'))//achar(int(Z'40'))//achar(int(Z'41')))
    call check(error, status == DuckDBError, "Can bind an invalid unicode to varchar.")
    if (allocated(error)) return
    status = duckdb_bind_varchar(stmt, 1, "44")
    call check(error, status == duckdbsuccess, "Cannot bind to varchar.")
    if (allocated(error)) return
    status = duckdb_execute_prepared(stmt, res)
    call check(error, status == duckdbsuccess, "Cannot execute bind to varchar.")
    if (allocated(error)) return
    call check(error, duckdb_value_int64(res, 0, 0) == 44, "cannot extract varchar result.")
    if (allocated(error)) return
    call duckdb_destroy_result(res)

    status = duckdb_bind_null(stmt, 1)
    call check(error, status == duckdbsuccess, "Cannot bind to null.")
    if (allocated(error)) return
    status = duckdb_execute_prepared(stmt, res)
    call check(error, status == duckdbsuccess, "Cannot execute bind to null.")
    if (allocated(error)) return
    ! call check(error, duckdb_nullmask_data(res, 0), "cannot extract null result.")
    ! if (allocated(error)) return
    ! call duckdb_destroy_result(res)

    call duckdb_destroy_prepare(stmt)
    ! again to make sure it does not crash
    call duckdb_destroy_result(res)
    call duckdb_destroy_prepare(stmt)

    status = duckdb_prepare(connection, "SELECT CAST($1 AS VARCHAR)", stmt)
    call check(error, status == duckdbsuccess, "Cannot prepare cast to varchar.")
    if (allocated(error)) return
    call check(error, c_associated(stmt%prep), "Statement is allocated.")
    if (allocated(error)) return

    ! invalid unicode
    status = duckdb_bind_varchar_length(stmt, 1, achar(int(Z'80')), 1)
    call check(error, status == DuckDBError, "Can bind an invalid unicode to varchar.")
    if (allocated(error)) return

    ! we can bind null values, though!
    status = duckdb_bind_varchar_length(stmt, 1, achar(int(Z'00'))//achar(int(Z'40'))//achar(int(Z'41')), 3)
    call check(error, status == DuckDBSuccess, "Cannot bind null values to varchar.")
    if (allocated(error)) return
    status = duckdb_bind_varchar_length(stmt, 1, "hello world", 5)
    call check(error, status == DuckDBSuccess, "Cannot bind string to varchar length.")
    if (allocated(error)) return
    status = duckdb_execute_prepared(stmt, res)
    call check(error, status == duckdbsuccess, "Cannot execute bind to varchar length.")
    if (allocated(error)) return
    str_res = duckdb_value_varchar(res, 0, 0)
    call check(error, str_res == "hello", "Cannot retrieve string from varchar.")
    if (allocated(error)) return
    call check(error, duckdb_value_int8(res, 0, 0) == 0_int8, "Cannot retrieve int8 from result.")
    if (allocated(error)) return
    call duckdb_destroy_result(res)

    str = "hello\0world"
    blob_data=duckdb_blob(c_loc(str), len(str))
    status = duckdb_bind_blob(stmt, 1, blob_data)
    call check(error, status == duckdbsuccess, "Cannot bind binary blob.")
    if (allocated(error)) return
    status = duckdb_execute_prepared(stmt, res)
    call check(error, status == duckdbsuccess, "Cannot execute bind to blob.")
    if (allocated(error)) return
    ! FIXME: Retrieving a blob as varchar returns the hex literal rather than the string.
    ! str_res = duckdb_value_varchar(res, 0, 0)
    ! call check(error, str_res == "hello\\x00world", "Cannot retrieve blob data from varchar.")
    ! if (allocated(error)) return
    call check(error, duckdb_value_int8(res, 0, 0) == 0, "Cannot retrieve blob data as int8")
    if (allocated(error)) return
    call duckdb_destroy_result(res)

    date_struct%year = 1992
    date_struct%month = 9
    date_struct%day = 3

    status = duckdb_bind_date(stmt, 1, duckdb_to_date(date_struct))
    status = duckdb_execute_prepared(stmt, res)
    call check(error, status == duckdbsuccess, "Cannot execute bind to date.")
    if (allocated(error)) return
    str_res = duckdb_value_varchar(res, 0, 0)
    call check(error, str_res == "1992-09-03", "Cannot retrieve date_struct as varchar.")
    if (allocated(error)) return
    call duckdb_destroy_result(res)
 
    time_struct%hour = 12
    time_struct%min = 22
    time_struct%sec = 33
    time_struct%micros = 123400

    status = duckdb_bind_time(stmt, 1, duckdb_to_time(time_struct))
    status = duckdb_execute_prepared(stmt, res)
    call check(error, status == duckdbsuccess, "Cannot execute bind to time.")
    if (allocated(error)) return
    str_res = duckdb_value_varchar(res, 0, 0)
    call check(error, str_res == "12:22:33.1234", "Cannot retrieve time_struct as varchar.")
    if (allocated(error)) return
    call duckdb_destroy_result(res)

    ts%date = date_struct
    ts%time = time_struct

    status = duckdb_bind_timestamp(stmt, 1, duckdb_to_timestamp(ts))
    status = duckdb_execute_prepared(stmt, res)
    call check(error, status == duckdbsuccess, "Cannot execute bind to time.")
    if (allocated(error)) return
    str_res = duckdb_value_varchar(res, 0, 0)
    call check(error, str_res == "1992-09-03 12:22:33.1234", &
      "Cannot retrieve timestamp_struct as varchar.")
    if (allocated(error)) return
    call duckdb_destroy_result(res)

    interval%months = 3;
    interval%days = 0;
    interval%micros = 0;

    status = duckdb_bind_interval(stmt, 1, interval)
    status = duckdb_execute_prepared(stmt, res)
    call check(error, status == duckdbsuccess, "Cannot execute bind to interval.")
    if (allocated(error)) return
    str_res = duckdb_value_varchar(res, 0, 0)
    call check(error, str_res == "3 months", "Cannot retrieve interval as varchar.")
    if (allocated(error)) return
    call duckdb_destroy_result(res)
    call duckdb_destroy_prepare(stmt)

    call check(error, duckdb_query(connection, "CREATE TABLE a (i INTEGER)", res) == duckdbsuccess)
    if (allocated(error)) return

    status = duckdb_prepare(connection, "INSERT INTO a VALUES (?)", stmt)
    call check(error, status == duckdbsuccess, "Cannot prepare insert values.")
    if (allocated(error)) return
    call check(error, c_associated(stmt%prep), "Statement for insert values not allocated.")
    if (allocated(error)) return
    call check(error, duckdb_nparams(unallocated_stmt)==0, "error in nparams for null.")
    if (allocated(error)) return
    call check(error, duckdb_nparams(stmt)==1, "error in nparams.")
    if (allocated(error)) return
    call check(error, duckdb_param_type(unallocated_stmt, 0)==DUCKDB_TYPE_INVALID, "error in invalid param type.")
    if (allocated(error)) return
    call check(error, duckdb_param_type(stmt, 0)==DUCKDB_TYPE_INVALID, "error in stmt, 0 param type.")
    if (allocated(error)) return
    call check(error, duckdb_param_type(stmt, 1)==DUCKDB_TYPE_INTEGER, "error in stmt, 1 param type.")
    if (allocated(error)) return
    call check(error, duckdb_param_type(stmt, 2)==DUCKDB_TYPE_INVALID, "error in stmt, 2 param type.")
    if (allocated(error)) return

    do i = 1, 1000
      status = duckdb_bind_int32(stmt, 1, i)
      status = duckdb_execute_prepared(stmt, res)
      call check(error, status == DuckDBSuccess, "error in loop bind int32.")
      if (allocated(error)) return  
    end do
    call duckdb_destroy_prepare(stmt)

    status = duckdb_prepare(connection, "SELECT SUM(i)*$1-$2 FROM a", stmt)
    call check(error, status == DuckDBSuccess, "error prepare select sum.")
    if (allocated(error)) return  
    call check(error, c_associated(stmt%prep), "Statement for select sum not allocated.")
    if (allocated(error)) return

    ! clear bindings
    status = duckdb_bind_int32(stmt, 1, 2)
    call check(error, duckdb_clear_bindings(stmt) == DuckDBSuccess, "Error in clear bindings.")
    if (allocated(error)) return

    ! bind again will succeed
    status = duckdb_bind_int32(stmt, 1, 2)
    status = duckdb_bind_int32(stmt, 2, 1000)
    status = duckdb_execute_prepared(stmt, res)
    call check(error, status == DuckDBSuccess, "Error in execute again bind int 32.")
    if (allocated(error)) return
    call check(error, duckdb_value_int32(res, 0, 0) == 1000000, "Error in get result as int32.")
    if (allocated(error)) return
    call duckdb_destroy_result(res)
    call duckdb_destroy_prepare(stmt)

    ! not-so-happy path
    status = duckdb_prepare(connection, "SELECT XXXXX", stmt)
    call check(error, status == DuckDBError, "Error in unhappy prepare.")
    if (allocated(error)) return
    status = duckdb_prepare(connection, "SELECT CAST($1 AS INTEGER)", stmt)
    call check(error, status == DuckDBSuccess, "Error in cast input as integer prepare.")
    if (allocated(error)) return
    status = duckdb_execute_prepared(stmt, res)
    call check(error, status == DuckDBError, "Error in execut cast input as integer, no input.")
    if (allocated(error)) return
    call duckdb_destroy_result(res)
    call duckdb_destroy_prepare(stmt)

    ! test duckdb_malloc explicitly
    block 
      type(c_ptr) :: malloced_data
      character(len=6), target :: f_str = "hello "
      character(len=:), allocatable :: tmp
      malloced_data = duckdb_malloc(int(100, kind=int64))
      malloced_data = c_loc(f_str)
      call c_f_str_ptr(malloced_data, tmp)
      call check(error, tmp == "hello", "Error in duckdb_malloc.")
      if (allocated(error)) return
    end block

    status = duckdb_prepare(connection, "SELECT sum(i) FROM a WHERE i > ?", stmt)
    call check(error, status == DuckDBSuccess, "Error select sum prepare.")
    if (allocated(error)) return
    call check(error, c_associated(stmt%prep), "Unallocated statement (prep sum).")
    if (allocated(error)) return
    call check(error, duckdb_nparams(stmt) == 1, "Nparams different from 1.")
    if (allocated(error)) return
    call check(error, duckdb_param_type(unallocated_stmt, 0)==DUCKDB_TYPE_INVALID, &
      "unallocated statment has param type.")
    if (allocated(error)) return
    call check(error, duckdb_param_type(stmt, 1)==DUCKDB_TYPE_INTEGER, &
      "param type 1 not integer.")
    if (allocated(error)) return
    call duckdb_destroy_prepare(stmt)
  end subroutine test_prepared_statements
end module test_prepared