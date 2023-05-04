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
    type(duckdb_appender) :: tappender 
    integer(kind=kind(duckdb_state)) :: status

    type(duckdb_date_struct) :: date_struct
    type(duckdb_time_struct) :: time_struct
    type(duckdb_timestamp_struct) :: ts
    type(duckdb_interval) :: interval
    type(duckdb_blob) :: blob_data

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

    status = duckdb_append_varchar(appender, "Hello, World")
    call check(error, status == duckdbsuccess, "duckdb_append_varchar 2 error.")
    if (allocated(error)) return 

    ! Out of columns.
    status = duckdb_append_int32(appender, 42)
    call check(error, status == duckdberror, "duckdb_append_int32 3 should fail.")
    if (allocated(error)) return 

    call check(error, duckdb_appender_error(appender) /= "NULL")
    if (allocated(error)) return

    status = duckdb_appender_end_row(appender)
    call check(error, status == duckdbsuccess)
    if (allocated(error)) return 

    ! we can flush again why not
    status = duckdb_appender_flush(appender)
    call check(error, status == duckdbsuccess)
    if (allocated(error)) return 

    status = duckdb_appender_close(appender)
    call check(error, status == duckdbsuccess)
    if (allocated(error)) return 

    status = duckdb_query(con, "SELECT * FROM test", result)
    call check(error, status == duckdbsuccess, "Query gives error.")
    if (allocated(error)) return 

    call check(error, duckdb_value_int32(result, 0, 0) == 42)
    if (allocated(error)) return 

    call check(error, abs(duckdb_value_double(result, 1, 0) - 4.2_real64) < 1e-3)
    if (allocated(error)) return 

    ! FIXME duckdb_value_string returns a duckdb_string. Should we return a character array?
    ! call check(error, duckdb_value_string(result, 2, 0) == "Hello, World")
    ! if (allocated(error)) return 

    status = duckdb_appender_destroy(appender)
    call check(error, status == duckdbsuccess)
    if (allocated(error)) return 

    !! Working with a destroyed appender should return errors
    status = duckdb_appender_close(appender)
    call check(error, status == duckdberror)
    if (allocated(error)) return 
    call check(error, duckdb_appender_error(appender) == "NULL")
    if (allocated(error)) return

    status = duckdb_appender_flush(appender)
    call check(error, status == duckdberror)
    if (allocated(error)) return 

    status = duckdb_appender_end_row(appender)
    call check(error, status == duckdberror)
    if (allocated(error)) return 

    status = duckdb_append_int32(appender, 42)
    call check(error, status == duckdberror)
    if (allocated(error)) return 

    status = duckdb_appender_destroy(appender)
    call check(error, status == duckdberror)
    if (allocated(error)) return 

    status = duckdb_appender_close(a)
    call check(error, status == duckdberror)
    if (allocated(error)) return 

    status = duckdb_appender_flush(a)
    call check(error, status == duckdberror)
    if (allocated(error)) return 

    status = duckdb_appender_end_row(a)
    call check(error, status == duckdberror)
    if (allocated(error)) return 

    status = duckdb_append_int32(a, 42)
    call check(error, status == duckdberror)
    if (allocated(error)) return 

    status = duckdb_appender_destroy(a)
    call check(error, status == duckdberror)
    if (allocated(error)) return 

    ! Note: removed the unsigned types from the original C++ test
    call check(error, duckdb_query(con, "CREATE TABLE many_types(bool boolean,  &
      &t TINYINT, s SMALLINT, b BIGINT, uf REAL, ud DOUBLE, txt VARCHAR,        &
      &blb BLOB, dt DATE, tm TIME, ts TIMESTAMP, ival INTERVAL, h HUGEINT)",    &
      result) == duckdbsuccess)
    if (allocated(error)) return 

    status = duckdb_appender_create(con, "", "many_types", tappender)
    call check(error, status == duckdbsuccess)
    if (allocated(error)) return 

    status = duckdb_appender_begin_row(tappender)
    call check(error, status == duckdbsuccess, "duckdb_appender_begin_row error.")
    if (allocated(error)) return 

    status = duckdb_append_bool(tappender, .true.)
    call check(error, status == duckdbsuccess, "duckdb_append_bool error.")
    if (allocated(error)) return 

    status = duckdb_append_int8(tappender, 1_int8)
    call check(error, status == duckdbsuccess, "duckdb_append_int8 error.")
    if (allocated(error)) return 
  
    status = duckdb_append_int16(tappender, 1_int16)
    call check(error, status == duckdbsuccess, "duckdb_append_int16 error.")
    if (allocated(error)) return 
  
    status = duckdb_append_int64(tappender, 1_int64)
    call check(error, status == duckdbsuccess, "duckdb_append_int64 error.")
    if (allocated(error)) return   

    ! uint functions are not implemented. 
    ! status = duckdb_append_uint8(tappender, 1);
    ! status = duckdb_append_uint16(tappender, 1);
    ! status = duckdb_append_uint32(tappender, 1);
    ! status = duckdb_append_uint64(tappender, 1);
  
    status = duckdb_append_float(tappender, 0.5_real32);
    call check(error, status == duckdbsuccess, "duckdb_append_float error.")
    if (allocated(error)) return  
  
    status = duckdb_append_double(tappender, 0.5_real64);
    call check(error, status == duckdbsuccess, "duckdb_append_float error.")
    if (allocated(error)) return  
  
    status = duckdb_append_varchar(tappender, "hello world");
    call check(error, status == duckdbsuccess, "duckdb_append_varchar error.")
    if (allocated(error)) return  

    date_struct%year = int(1992, kind=c_int32_t)
    date_struct%month = int(9, kind=c_int8_t)
    date_struct%day = int(3, kind=c_int8_t)

    block 
      character(len=:), allocatable, target :: tmp 
      tmp = "hello world this\0is my long string"
      blob_data%data = c_loc(tmp)
      blob_data%size = len(tmp, kind=c_int64_t)
    end block 
    status = duckdb_append_blob(tappender, blob_data)
    call check(error, status == duckdbsuccess, "duckdb_append_blob error.")
    if (allocated(error)) return  

    status = duckdb_append_date(tappender, duckdb_to_date(date_struct))
    call check(error, status == duckdbsuccess, "duckdb_append_date error.")
    if (allocated(error)) return  

    time_struct%hour = int(12, kind=c_int8_t)
    time_struct%min = int(22, kind=c_int8_t)
    time_struct%sec = int(33, kind=c_int8_t)
    time_struct%micros = int(1234, kind=c_int32_t)

    status = duckdb_append_time(tappender, duckdb_to_time(time_struct))
    call check(error, status == duckdbsuccess, "duckdb_append_time error.")
    if (allocated(error)) return  

    ts%date = date_struct
    ts%time = time_struct

    status = duckdb_append_timestamp(tappender, duckdb_to_timestamp(ts))
    call check(error, status == duckdbsuccess, "duckdb_append_timestamp error.")
    if (allocated(error)) return 

    interval%months = int(3, kind=c_int32_t)
    interval%days = int(0, kind=c_int32_t)
    interval%micros = int(0, kind=c_int64_t)

    status = duckdb_append_interval(tappender, interval)
    call check(error, status == duckdbsuccess, "duckdb_append_interval error.")
    if (allocated(error)) return 

    status = duckdb_append_hugeint(tappender, duckdb_double_to_hugeint(27.0_real64))
    call check(error, status == duckdbsuccess, "duckdb_append_hugeint error.")
    if (allocated(error)) return 

    status = duckdb_appender_end_row(tappender)
    call check(error, status == duckdbsuccess)
    if (allocated(error)) return 

    status = duckdb_appender_begin_row(tappender)
    call check(error, status == duckdbsuccess, "duckdb_appender_begin_row 2 error.")
    if (allocated(error)) return 

    status = duckdb_append_null(tappender)
    call check(error, status == duckdbsuccess, "duckdb_append_null error.")
    if (allocated(error)) return 

    status = duckdb_append_null(tappender)
    call check(error, status == duckdbsuccess, "duckdb_append_null error.")
    if (allocated(error)) return 

    status = duckdb_append_null(tappender)
    call check(error, status == duckdbsuccess, "duckdb_append_null error.")
    if (allocated(error)) return 

    status = duckdb_append_null(tappender)
    call check(error, status == duckdbsuccess, "duckdb_append_null error.")
    if (allocated(error)) return 

    status = duckdb_append_null(tappender)
    call check(error, status == duckdbsuccess, "duckdb_append_null error.")
    if (allocated(error)) return 

    status = duckdb_append_null(tappender)
    call check(error, status == duckdbsuccess, "duckdb_append_null error.")
    if (allocated(error)) return 

    status = duckdb_append_null(tappender)
    call check(error, status == duckdbsuccess, "duckdb_append_null error.")
    if (allocated(error)) return 

    status = duckdb_append_null(tappender)
    call check(error, status == duckdbsuccess, "duckdb_append_null error.")
    if (allocated(error)) return 

    status = duckdb_append_null(tappender)
    call check(error, status == duckdbsuccess, "duckdb_append_null error.")
    if (allocated(error)) return 

    status = duckdb_append_null(tappender)
    call check(error, status == duckdbsuccess, "duckdb_append_null error.")
    if (allocated(error)) return 

    status = duckdb_append_null(tappender)
    call check(error, status == duckdbsuccess, "duckdb_append_null error.")
    if (allocated(error)) return 

    status = duckdb_append_null(tappender)
    call check(error, status == duckdbsuccess, "duckdb_append_null error.")
    if (allocated(error)) return 

    status = duckdb_append_null(tappender)
    call check(error, status == duckdbsuccess, "duckdb_append_null error.")
    if (allocated(error)) return 

    status = duckdb_appender_end_row(tappender)
    call check(error, status == duckdbsuccess, "duckdb_appender_end_row error.")
    if (allocated(error)) return 

    status = duckdb_appender_flush(tappender)
    call check(error, status == duckdbsuccess, "duckdb_appender_flush error.")
    if (allocated(error)) return 

    status = duckdb_appender_close(tappender)
    call check(error, status == duckdbsuccess)
    if (allocated(error)) return 

    call check(error, duckdb_appender_destroy(tappender) == duckdbsuccess, &
      "Appender not destroyed successfully.")
    if (allocated(error)) return

    ! result = tester.Query("SELECT * FROM many_types");
    ! REQUIRE_NO_FAIL(*result);
    ! REQUIRE(result->Fetch<bool>(0, 0) == true);
    ! REQUIRE(result->Fetch<int8_t>(1, 0) == 1);
    ! REQUIRE(result->Fetch<int16_t>(2, 0) == 1);
    ! REQUIRE(result->Fetch<int64_t>(3, 0) == 1);
    ! REQUIRE(result->Fetch<uint8_t>(4, 0) == 1);
    ! REQUIRE(result->Fetch<uint16_t>(5, 0) == 1);
    ! REQUIRE(result->Fetch<uint32_t>(6, 0) == 1);
    ! REQUIRE(result->Fetch<uint64_t>(7, 0) == 1);
    ! REQUIRE(result->Fetch<float>(8, 0) == 0.5f);
    ! REQUIRE(result->Fetch<double>(9, 0) == 0.5);
    ! REQUIRE(result->Fetch<string>(10, 0) == "hello");
  
    ! auto blob = duckdb_value_blob(&result->InternalResult(), 11, 0);
    ! REQUIRE(blob.size == blob_len);
    ! REQUIRE(memcmp(blob.data, blob_data, blob_len) == 0);
    ! duckdb_free(blob.data);
    ! REQUIRE(duckdb_value_int32(&result->InternalResult(), 11, 0) == 0);
  
    ! auto date = result->Fetch<duckdb_date_struct>(12, 0);
    ! REQUIRE(date.year == 1992);
    ! REQUIRE(date.month == 9);
    ! REQUIRE(date.day == 3);
  
    ! auto time = result->Fetch<duckdb_time_struct>(13, 0);
    ! REQUIRE(time.hour == 12);
    ! REQUIRE(time.min == 22);
    ! REQUIRE(time.sec == 33);
    ! REQUIRE(time.micros == 1234);
  
    ! auto timestamp = result->Fetch<duckdb_timestamp_struct>(14, 0);
    ! REQUIRE(timestamp.date.year == 1992);
    ! REQUIRE(timestamp.date.month == 9);
    ! REQUIRE(timestamp.date.day == 3);
    ! REQUIRE(timestamp.time.hour == 12);
    ! REQUIRE(timestamp.time.min == 22);
    ! REQUIRE(timestamp.time.sec == 33);
    ! REQUIRE(timestamp.time.micros == 1234);
  
    ! interval = result->Fetch<duckdb_interval>(15, 0);
    ! REQUIRE(interval.months == 3);
    ! REQUIRE(interval.days == 0);
    ! REQUIRE(interval.micros == 0);
  
    ! auto hugeint = result->Fetch<duckdb_hugeint>(16, 0);
    ! REQUIRE(duckdb_hugeint_to_double(hugeint) == 27);
  
    ! REQUIRE(result->IsNull(0, 1));
    ! REQUIRE(result->IsNull(1, 1));
    ! REQUIRE(result->IsNull(2, 1));
    ! REQUIRE(result->IsNull(3, 1));
    ! REQUIRE(result->IsNull(4, 1));
    ! REQUIRE(result->IsNull(5, 1));
    ! REQUIRE(result->IsNull(6, 1));
    ! REQUIRE(result->IsNull(7, 1));
    ! REQUIRE(result->IsNull(8, 1));
    ! REQUIRE(result->IsNull(9, 1));
    ! REQUIRE(result->IsNull(10, 1));
    ! REQUIRE(result->IsNull(11, 1));
    ! REQUIRE(result->IsNull(12, 1));
    ! REQUIRE(result->IsNull(13, 1));
    ! REQUIRE(result->IsNull(14, 1));
    ! REQUIRE(result->IsNull(15, 1));
    ! REQUIRE(result->IsNull(16, 1));
  
    ! REQUIRE(result->Fetch<bool>(0, 1) == false);
    ! REQUIRE(result->Fetch<int8_t>(1, 1) == 0);
    ! REQUIRE(result->Fetch<int16_t>(2, 1) == 0);
    ! REQUIRE(result->Fetch<int64_t>(3, 1) == 0);
    ! REQUIRE(result->Fetch<uint8_t>(4, 1) == 0);
    ! REQUIRE(result->Fetch<uint16_t>(5, 1) == 0);
    ! REQUIRE(result->Fetch<uint32_t>(6, 1) == 0);
    ! REQUIRE(result->Fetch<uint64_t>(7, 1) == 0);
    ! REQUIRE(result->Fetch<float>(8, 1) == 0);
    ! REQUIRE(result->Fetch<double>(9, 1) == 0);
    ! REQUIRE(result->Fetch<string>(10, 1).empty());
  
    ! blob = duckdb_value_blob(&result->InternalResult(), 11, 1);
    ! REQUIRE(blob.size == 0);
  
    ! date = result->Fetch<duckdb_date_struct>(12, 1);
    ! REQUIRE(date.year == 1970);
  
    ! time = result->Fetch<duckdb_time_struct>(13, 1);
    ! REQUIRE(time.hour == 0);
  
    ! timestamp = result->Fetch<duckdb_timestamp_struct>(14, 1);
    ! REQUIRE(timestamp.date.year == 1970);
    ! REQUIRE(timestamp.time.hour == 0);
  
    ! interval = result->Fetch<duckdb_interval>(15, 1);
    ! REQUIRE(interval.months == 0);
  
    ! hugeint = result->Fetch<duckdb_hugeint>(16, 1);
    ! REQUIRE(duckdb_hugeint_to_double(hugeint) == 0);
  
    ! // double out of range for hugeint
    ! hugeint = duckdb_double_to_hugeint(1e300);
    ! REQUIRE(hugeint.lower == 0);
    ! REQUIRE(hugeint.upper == 0);
  
    ! hugeint = duckdb_double_to_hugeint(NAN);
    ! REQUIRE(hugeint.lower == 0);
    ! REQUIRE(hugeint.upper == 0);
  end subroutine test_appender_statements
end module test_appender
