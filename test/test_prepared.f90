module test_prepared
  use, intrinsic :: iso_c_binding, only: c_associated
  use, intrinsic :: ieee_arithmetic, only: ieee_value, ieee_quiet_nan
  use constants
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
    type(duckdb_prepared_statement) :: stmt
    type(duckdb_result) :: res
    integer(kind(duckdb_state)) :: status
    type(duckdb_decimal) :: decimal, result_decimal

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

  end subroutine test_prepared_statements
end module test_prepared


! 	status = duckdb_prepare(tester.connection, "SELECT CAST($1 AS VARCHAR)", &stmt);
! 	REQUIRE(status == DuckDBSuccess);
! 	REQUIRE(stmt != nullptr);

! 	// invalid unicode
! 	REQUIRE(duckdb_bind_varchar_length(stmt, 1, "\x80", 1) == DuckDBError);
! 	// we can bind null values, though!
! 	REQUIRE(duckdb_bind_varchar_length(stmt, 1, "\x00\x40\x41", 3) == DuckDBSuccess);
! 	duckdb_bind_varchar_length(stmt, 1, "hello world", 5);
! 	status = duckdb_execute_prepared(stmt, &res);
! 	REQUIRE(status == DuckDBSuccess);
! 	auto value = duckdb_value_varchar(&res, 0, 0);
! 	REQUIRE(string(value) == "hello");
! 	REQUIRE(duckdb_value_int8(&res, 0, 0) == 0);
! 	duckdb_free(value);
! 	duckdb_destroy_result(&res);

! 	duckdb_bind_blob(stmt, 1, "hello\0world", 11);
! 	status = duckdb_execute_prepared(stmt, &res);
! 	REQUIRE(status == DuckDBSuccess);
! 	value = duckdb_value_varchar(&res, 0, 0);
! 	REQUIRE(string(value) == "hello\\x00world");
! 	REQUIRE(duckdb_value_int8(&res, 0, 0) == 0);
! 	duckdb_free(value);
! 	duckdb_destroy_result(&res);

! 	duckdb_date_struct date_struct;
! 	date_struct.year = 1992;
! 	date_struct.month = 9;
! 	date_struct.day = 3;

! 	duckdb_bind_date(stmt, 1, duckdb_to_date(date_struct));
! 	status = duckdb_execute_prepared(stmt, &res);
! 	REQUIRE(status == DuckDBSuccess);
! 	value = duckdb_value_varchar(&res, 0, 0);
! 	REQUIRE(string(value) == "1992-09-03");
! 	duckdb_free(value);
! 	duckdb_destroy_result(&res);

! 	duckdb_time_struct time_struct;
! 	time_struct.hour = 12;
! 	time_struct.min = 22;
! 	time_struct.sec = 33;
! 	time_struct.micros = 123400;

! 	duckdb_bind_time(stmt, 1, duckdb_to_time(time_struct));
! 	status = duckdb_execute_prepared(stmt, &res);
! 	REQUIRE(status == DuckDBSuccess);
! 	value = duckdb_value_varchar(&res, 0, 0);
! 	REQUIRE(string(value) == "12:22:33.1234");
! 	duckdb_free(value);
! 	duckdb_destroy_result(&res);

! 	duckdb_timestamp_struct ts;
! 	ts.date = date_struct;
! 	ts.time = time_struct;

! 	duckdb_bind_timestamp(stmt, 1, duckdb_to_timestamp(ts));
! 	status = duckdb_execute_prepared(stmt, &res);
! 	REQUIRE(status == DuckDBSuccess);
! 	value = duckdb_value_varchar(&res, 0, 0);
! 	REQUIRE(string(value) == "1992-09-03 12:22:33.1234");
! 	duckdb_free(value);
! 	duckdb_destroy_result(&res);

! 	duckdb_interval interval;
! 	interval.months = 3;
! 	interval.days = 0;
! 	interval.micros = 0;

! 	duckdb_bind_interval(stmt, 1, interval);
! 	status = duckdb_execute_prepared(stmt, &res);
! 	REQUIRE(status == DuckDBSuccess);
! 	value = duckdb_value_varchar(&res, 0, 0);
! 	REQUIRE(string(value) == "3 months");
! 	duckdb_free(value);
! 	duckdb_destroy_result(&res);

! 	duckdb_destroy_prepare(&stmt);

! 	status = duckdb_query(tester.connection, "CREATE TABLE a (i INTEGER)", NULL);
! 	REQUIRE(status == DuckDBSuccess);

! 	status = duckdb_prepare(tester.connection, "INSERT INTO a VALUES (?)", &stmt);
! 	REQUIRE(status == DuckDBSuccess);
! 	REQUIRE(stmt != nullptr);
! 	REQUIRE(duckdb_nparams(nullptr) == 0);
! 	REQUIRE(duckdb_nparams(stmt) == 1);
! 	REQUIRE(duckdb_param_type(nullptr, 0) == DUCKDB_TYPE_INVALID);
! 	REQUIRE(duckdb_param_type(stmt, 0) == DUCKDB_TYPE_INVALID);
! 	REQUIRE(duckdb_param_type(stmt, 1) == DUCKDB_TYPE_INTEGER);
! 	REQUIRE(duckdb_param_type(stmt, 2) == DUCKDB_TYPE_INVALID);

! 	for (int32_t i = 1; i <= 1000; i++) {
! 		duckdb_bind_int32(stmt, 1, i);
! 		status = duckdb_execute_prepared(stmt, nullptr);
! 		REQUIRE(status == DuckDBSuccess);
! 	}
! 	duckdb_destroy_prepare(&stmt);

! 	status = duckdb_prepare(tester.connection, "SELECT SUM(i)*$1-$2 FROM a", &stmt);
! 	REQUIRE(status == DuckDBSuccess);
! 	REQUIRE(stmt != nullptr);
! 	// clear bindings
! 	duckdb_bind_int32(stmt, 1, 2);
! 	REQUIRE(duckdb_clear_bindings(stmt) == DuckDBSuccess);

! 	// bind again will succeed
! 	duckdb_bind_int32(stmt, 1, 2);
! 	duckdb_bind_int32(stmt, 2, 1000);
! 	status = duckdb_execute_prepared(stmt, &res);
! 	REQUIRE(status == DuckDBSuccess);
! 	REQUIRE(duckdb_value_int32(&res, 0, 0) == 1000000);
! 	duckdb_destroy_result(&res);
! 	duckdb_destroy_prepare(&stmt);

! 	// not-so-happy path
! 	status = duckdb_prepare(tester.connection, "SELECT XXXXX", &stmt);
! 	REQUIRE(status == DuckDBError);
! 	duckdb_destroy_prepare(&stmt);

! 	status = duckdb_prepare(tester.connection, "SELECT CAST($1 AS INTEGER)", &stmt);
! 	REQUIRE(status == DuckDBSuccess);
! 	REQUIRE(stmt != nullptr);

! 	status = duckdb_execute_prepared(stmt, &res);
! 	REQUIRE(status == DuckDBError);
! 	duckdb_destroy_result(&res);
! 	duckdb_destroy_prepare(&stmt);

! 	// test duckdb_malloc explicitly
! 	auto malloced_data = duckdb_malloc(100);
! 	memcpy(malloced_data, "hello\0", 6);
! 	REQUIRE(string((char *)malloced_data) == "hello");
! 	duckdb_free(malloced_data);

! 	status = duckdb_prepare(tester.connection, "SELECT sum(i) FROM a WHERE i > ?", &stmt);
! 	REQUIRE(status == DuckDBSuccess);
! 	REQUIRE(stmt != nullptr);
! 	REQUIRE(duckdb_nparams(stmt) == 1);
! 	REQUIRE(duckdb_param_type(nullptr, 0) == DUCKDB_TYPE_INVALID);
! 	REQUIRE(duckdb_param_type(stmt, 1) == DUCKDB_TYPE_INTEGER);

! 	duckdb_destroy_prepare(&stmt);
! }