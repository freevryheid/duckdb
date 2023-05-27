module test_appender

  use, intrinsic :: iso_c_binding
  use util
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
                new_unittest("appender-statements-test", test_appender_statements), &
                new_unittest("append-timestamp-test", test_append_timestamp) &
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

    character(len=:), allocatable, target :: tmp

    ! Open db in in-memory mode
    call check(error, duckdb_open("", db) == duckdbsuccess, "Could not open db.")
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
    ! not sure we want to 'fix' this
    ! status = duckdb_appender_create(con, "", "test", c_null_ptr)
    ! call check(error, status == duckdberror, "Appender did not return error.")
    ! if (allocated(error)) return
    ! status = duckdb_appender_create(tester.connection, nullptr, "test", nullptr);
    ! REQUIRE(status == DuckDBError);

    status = duckdb_appender_create(con, "", "test", appender)
    call check(error, status == duckdbsuccess, "Appender creation error.")
    if (allocated(error)) return

    call check(error, duckdb_appender_error(appender) == "", "Appender has error message.")
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

    call check(error, duckdb_appender_error(appender) /= "")
    if (allocated(error)) return

    status = duckdb_append_varchar(appender, "Hello, World")
    call check(error, status == duckdbsuccess, "duckdb_append_varchar 2 error.")
    if (allocated(error)) return

    ! Out of columns.
    status = duckdb_append_int32(appender, 42)
    call check(error, status == duckdberror, "duckdb_append_int32 3 should fail.")
    if (allocated(error)) return

    call check(error, duckdb_appender_error(appender) /= "")
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

    call check(error, duckdb_string_to_character(duckdb_value_string(result, 2, 0)) &
      == "Hello, World")
    if (allocated(error)) return

    status = duckdb_appender_destroy(appender)
    call check(error, status == duckdbsuccess)
    if (allocated(error)) return

    !! Working with a destroyed appender should return errors
    status = duckdb_appender_close(appender)
    call check(error, status == duckdberror)
    if (allocated(error)) return
    call check(error, duckdb_appender_error(appender) == "")
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

    ! FIXME - cannot use a block here else tmp will be freed on block exit (right?)
    ! but we want to retain the pointer to it
    ! block
      ! character(len=:), allocatable, target :: tmp

    tmp = "hello world this\0is my long string"
    blob_data%data = c_loc(tmp)

    blob_data%size = len(tmp, kind=c_int64_t)
    ! end block

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

    result = duckdb_result()
    call check(error, duckdb_query(con, "SELECT * FROM many_types", result) == duckdbsuccess)
    if (allocated(error)) return

    call check(error, duckdb_value_boolean(result, 0, 0) .eqv. .true., "error retrieving logical")
    if (allocated(error)) return
    call check(error, duckdb_value_int8(result, 1, 0) == 1_int8, "error retrieving int8")
    if (allocated(error)) return
    call check(error, duckdb_value_int16(result, 2, 0) == 1_int16, "error retrieving int16")
    if (allocated(error)) return
    call check(error, duckdb_value_int64(result, 3, 0) == 1_int64, "error retrieving int64")
    if (allocated(error)) return
    call check(error, duckdb_value_float(result, 4, 0) == 0.5_real32, "error retrieving real32")
    if (allocated(error)) return
    call check(error, duckdb_value_double(result, 5, 0) == 0.5_real64, "error retrieving real64")
    if (allocated(error)) return
    block
      character(len=:), allocatable :: val
      val = duckdb_string_to_character(duckdb_value_string(result, 6, 0))
      call check(error, val == "hello world", "error retrieving string")
      if (allocated(error)) return
    end block

    block
      type(duckdb_blob) :: blob
      character(len=:), pointer :: tmp1, tmp2
      blob = duckdb_value_blob(result, 7, 0)
      call check(error, blob%size == blob_data%size, "error retrieving blob size")
      if (allocated(error)) return
      call c_f_pointer(blob%data, tmp1)
      call c_f_pointer(blob_data%data, tmp2)
      tmp1 => tmp1(1:blob%size)
      tmp2 => tmp2(1:blob_data%size)
      call check(error, tmp1 == tmp2, "error retrieving blob data")
      if (allocated(error)) return
      ! FIXME: investigate duckdb_free further
      ! call duckdb_free(blob%data)
      ! call duckdb_free(blob_data%data)
      ! if we want to free these i think in fortran we could instead:
      tmp1 => null()
      tmp2 => null()
      blob = duckdb_blob()
      blob_data = duckdb_blob()
    end block

    call check(error, duckdb_value_int32(result, 7, 0) == 0)
    if (allocated(error)) return

    block
      type(duckdb_date_struct) :: date
      date = duckdb_from_date(duckdb_value_date(result, 8, 0))
      call check(error, date%year == 1992)
      if (allocated(error)) return
      call check(error, date%month == 9)
      if (allocated(error)) return
      call check(error, date%day == 3)
      if (allocated(error)) return
    end block

    block
      type(duckdb_time_struct) :: time
      time = duckdb_from_time(duckdb_value_time(result, 9, 0))
      call check(error, time%hour == 12, "hour")
      if (allocated(error)) return
      call check(error, time%min == 22, "min")
      if (allocated(error)) return
      call check(error, time%sec == 33, "sec")
      if (allocated(error)) return
      call check(error, time%micros == 1234, "micros")
      if (allocated(error)) return
    end block

    block
      type(duckdb_timestamp_struct) :: timestamp
      timestamp = duckdb_from_timestamp(duckdb_value_timestamp(result, 10, 0))
      call check(error, timestamp%date%year == 1992)
      if (allocated(error)) return
      call check(error, timestamp%date%month == 9)
      if (allocated(error)) return
      call check(error, timestamp%date%day == 3)
      if (allocated(error)) return
      call check(error, timestamp%time%hour == 12)
      if (allocated(error)) return
      call check(error, timestamp%time%min == 22)
      if (allocated(error)) return
      call check(error, timestamp%time%sec == 33)
      if (allocated(error)) return
      call check(error, timestamp%time%micros == 1234)
      if (allocated(error)) return
    end block

    block
      type(duckdb_interval) :: interval
      interval = duckdb_value_interval(result, 11, 0)
      call check(error, interval%months == 3)
      if (allocated(error)) return
      call check(error, interval%days == 0)
      if (allocated(error)) return
      call check(error, interval%micros == 0)
      if (allocated(error)) return
    end block

    ! FIXME failing test - looks OK to me
    block
      type(duckdb_hugeint) :: hugeint
      hugeint = duckdb_value_hugeint(result, 12, 0)
      ! print*, duckdb_hugeint_to_double(hugeint)
      call check(error, duckdb_hugeint_to_double(hugeint) == 27.0_real64)
      if (allocated(error)) return
    end block

    call check(error, duckdb_value_is_null(result, 0, 1), "0 null")
    if (allocated(error)) return
    call check(error, duckdb_value_is_null(result, 1, 1), "1 null")
    if (allocated(error)) return
    call check(error, duckdb_value_is_null(result, 2, 1), "2 null")
    if (allocated(error)) return
    call check(error, duckdb_value_is_null(result, 3, 1), "3 null")
    if (allocated(error)) return
    call check(error, duckdb_value_is_null(result, 4, 1), "4 null")
    if (allocated(error)) return
    call check(error, duckdb_value_is_null(result, 5, 1), "5 null")
    if (allocated(error)) return
    call check(error, duckdb_value_is_null(result, 6, 1), "6 null")
    if (allocated(error)) return
    call check(error, duckdb_value_is_null(result, 7, 1), "7 null")
    if (allocated(error)) return
    call check(error, duckdb_value_is_null(result, 8, 1), "8 null")
    if (allocated(error)) return
    call check(error, duckdb_value_is_null(result, 9, 1), "9 null")
    if (allocated(error)) return
    call check(error, duckdb_value_is_null(result, 10, 1), "10 null")
    if (allocated(error)) return
    call check(error, duckdb_value_is_null(result, 11, 1), "11 null")
    if (allocated(error)) return
    call check(error, duckdb_value_is_null(result, 12, 1), "12 null")
    if (allocated(error)) return

    call check(error, duckdb_value_boolean(result, 0, 1) .eqv. .false.)
    if (allocated(error)) return
    call check(error, duckdb_value_int8(result, 1, 1) == 0)
    if (allocated(error)) return
    call check(error, duckdb_value_int16(result, 2, 1) == 0)
    if (allocated(error)) return
    call check(error, duckdb_value_int64(result, 3, 1) == 0)
    if (allocated(error)) return
    call check(error, duckdb_value_float(result, 4, 1) == 0)
    if (allocated(error)) return
    call check(error, duckdb_value_double(result, 5, 1) == 0)
    if (allocated(error)) return
    call check(error, duckdb_string_to_character(duckdb_value_string(result, 6, 1)) == "")
    if (allocated(error)) return

    block
      type(duckdb_blob) :: blob

      blob = duckdb_value_blob(result, 7, 1)
      call check(error, blob%size == 0)
      if (allocated(error)) return
      if (c_associated(blob%data)) call duckdb_free(blob%data)
    end block

    block
      type(duckdb_date_struct) :: date
      date = duckdb_from_date(duckdb_value_date(result, 8, 1))
      call check(error, date%year == 1970)
      if (allocated(error)) return
    end block

    block
      type(duckdb_time_struct) :: time
      time = duckdb_from_time(duckdb_value_time(result, 9, 1))
      call check(error, time%hour == 0)
      if (allocated(error)) return
    end block

    block
      type(duckdb_timestamp_struct) :: timestamp
      timestamp = duckdb_from_timestamp(duckdb_value_timestamp(result, 10, 1))
      call check(error, timestamp%date%year == 1970)
      if (allocated(error)) return
      call check(error, timestamp%time%hour == 0)
      if (allocated(error)) return
    end block

    block
      type(duckdb_interval) :: interval
      interval = duckdb_value_interval(result, 11, 1)
      call check(error, interval%months == 0)
      if (allocated(error)) return
    end block

    block
      ! real(kind=real64) :: hugeint
      type(duckdb_hugeint) :: hugeint
      hugeint = duckdb_value_hugeint(result, 12, 1)

      call check(error, duckdb_hugeint_to_double(hugeint) == 0)
      if (allocated(error)) return

      ! double out of range for hugeint
      ! hugeint = duckdb_double_to_hugeint(real(1e300, kind=real64))
      ! call check(error, hugeint%lower == 0)
      ! if (allocated(error)) return
      ! call check(error, hugeint%upper == 0)
      ! if (allocated(error)) return

      ! hugeint = duckdb_double_to_hugeint(1.0_real64/0.0_real64)
      ! call check(error, hugeint%lower == 0)
      ! if (allocated(error)) return
      ! call check(error, hugeint%upper == 0)
      ! if (allocated(error)) return
    end block
  end subroutine test_appender_statements

  subroutine test_append_timestamp(error)
    type(error_type), allocatable, intent(out) :: error
    type(duckdb_database) :: db
    type(duckdb_connection) :: con
    type(duckdb_result) :: result = duckdb_result()

    type(duckdb_appender) :: appender
    integer(kind=kind(duckdb_state)) :: status

    ! Open db in in-memory mode
    call check(error, duckdb_open("", db) == duckdbsuccess, "Could not open db.")
    if (allocated(error)) return
    call check(error, duckdb_connect(db, con) == duckdbsuccess, "Could not start connection.")
    if (allocated(error)) return

    call check(error, duckdb_query(con, &
                                   "CREATE TABLE test (t timestamp)", &
                                   result) /= duckdberror, "Could not run query.")
    if (allocated(error)) return

    status = duckdb_appender_create(con, "", "test", appender)
    call check(error, status == duckdbsuccess, "Appender did not return success.")
    if (allocated(error)) return

    call check(error, duckdb_appender_error(appender) == "", "Appender error message not null.")
    if (allocated(error)) return

    ! successfull append
    status = duckdb_appender_begin_row(appender)
    call check(error, status == duckdbsuccess, "New row did not return success.")
    if (allocated(error)) return

    ! status = duckdb_append_timestamp(appender, duckdb_timestamp{1649519797544000})
    status = duckdb_append_varchar(appender, "2022-04-09 15:56:37.544")
    call check(error, status == duckdbsuccess, "Append string did not return success.")
    if (allocated(error)) return

    status = duckdb_appender_end_row(appender)
    call check(error, status == duckdbsuccess, "End row did not return success.")
    if (allocated(error)) return

    ! append failure
    status = duckdb_appender_begin_row(appender)
    call check(error, status == duckdbsuccess, "New row did not return success.")
    if (allocated(error)) return

    status = duckdb_append_varchar(appender, "XXXXX")
    call check(error, status == DuckDBError, "XXXXX does not give error")
    if (allocated(error)) return
    call check(error, duckdb_appender_error(appender) /= "", "Appender error message not null.")
    if (allocated(error)) return

    status = duckdb_appender_end_row(appender)
    call check(error, status == DuckDBError, "End row does not give error")
    if (allocated(error)) return

    status = duckdb_appender_flush(appender)
    call check(error, status == DuckDBSuccess, "flush error")
    if (allocated(error)) return

    status = duckdb_appender_close(appender)
    call check(error, status == DuckDBSuccess, "close error")
    if (allocated(error)) return

    status = duckdb_appender_destroy(appender)
    call check(error, status == DuckDBSuccess, "destroy error")
    if (allocated(error)) return

    call check(error, duckdb_query(con, "SELECT * FROM test", result) &
      /= duckdberror, "Could not run query.")
    if (allocated(error)) return

    call check(error, duckdb_string_to_character(duckdb_value_string(result, 0, 0)) &
      == "2022-04-09 15:56:37.544")
    if (allocated(error)) return
  end subroutine test_append_timestamp
end module test_appender
