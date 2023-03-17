module duckdb
  use, intrinsic :: iso_c_binding,
  use, intrinsic :: iso_fortran_env, only : int8, int16, int32, int64, real32, real64, real128
  use util
  implicit none
  private
  public :: duckdb_date, duckdb_date_struct, duckdb_time, duckdb_time_struct, duckdb_timestamp
  public :: duckdb_timestamp_struct, duckdb_interval, duckdb_hugeint, duckdb_decimal, duckdb_string
  public :: duckdb_blob, duckdb_list_entry, duckdb_column, duckdb_result
  public :: duckdb_type, duckdb_type_invalid, duckdb_type_boolean, duckdb_type_tinyint
  public :: duckdb_type_smallint, duckdb_type_integer, duckdb_type_bigint, duckdb_type_utinyint
  public :: duckdb_type_usmallint, duckdb_type_uinteger, duckdb_type_ubigint, duckdb_type_float
  public :: duckdb_type_double, duckdb_type_timestamp, duckdb_type_date, duckdb_type_time
  public :: duckdb_type_interval, duckdb_type_hugeint, duckdb_type_varchar, duckdb_type_blob
  public :: duckdb_type_decimal, duckdb_type_timestamp_s, duckdb_type_timestamp_ms
  public :: duckdb_type_timestamp_ns, duckdb_type_enum, duckdb_type_list, duckdb_type_struct
  public :: duckdb_type_map, duckdb_type_uuid, duckdb_type_union, duckdb_type_bit
  public :: duckdb_pending_state, duckdb_pending_result_ready
  public :: duckdb_pending_result_not_ready, duckdb_pending_error
  public :: duckdb_state, duckdbsuccess, duckdberror
  public :: duckdb_open, duckdb_close
  public :: duckdb_connect, duckdb_disconnect
  public :: duckdb_query
  public :: duckdb_destroy_result
  public :: duckdb_column_count, duckdb_row_count, duckdb_rows_changed
  public :: duckdb_library_version
  public :: duckdb_column_name
  public :: duckdb_column_type
  public :: duckdb_column_data
  public :: duckdb_nullmask_data
  public :: duckdb_result_error
  public :: duckdb_result_get_chunk
  public :: duckdb_result_chunk_count
  public :: duckdb_value_boolean
  public :: duckdb_value_int8
  public :: duckdb_value_int16
  public :: duckdb_value_int32
  public :: duckdb_value_int64
  public :: duckdb_value_float
  public :: duckdb_value_double
  public :: duckdb_value_date
  public :: duckdb_value_time
  public :: duckdb_value_timestamp
  public :: duckdb_value_interval
  public :: duckdb_value_string
  public :: duckdb_value_blob
  public :: duckdb_value_is_null
  public :: duckdb_malloc, duckdb_free, duckdb_vector_size
  public :: duckdb_from_date, duckdb_to_date
  public :: duckdb_from_time, duckdb_to_time
  public :: duckdb_from_timestamp, duckdb_to_timestamp
  public :: duckdb_hugeint_to_double, duckdb_double_to_hugeint
  public :: duckdb_decimal_to_double, duckdb_double_to_decimal

  enum, bind(c)
    enumerator :: duckdb_state  = 0
    enumerator :: duckdbsuccess = 0
    enumerator :: duckdberror   = 1
  end enum

  enum, bind(c)
    enumerator :: duckdb_type              = 0
    enumerator :: duckdb_type_invalid      = 0
    enumerator :: duckdb_type_boolean      = 1  ! bool
    enumerator :: duckdb_type_tinyint      = 2  ! int8_t
    enumerator :: duckdb_type_smallint     = 3  ! int16_t
    enumerator :: duckdb_type_integer      = 4  ! int32_t
    enumerator :: duckdb_type_bigint       = 5  ! int64_t
    enumerator :: duckdb_type_utinyint     = 6  ! uint8_t
    enumerator :: duckdb_type_usmallint    = 7  ! uint16_t
    enumerator :: duckdb_type_uinteger     = 8  ! uint32_t
    enumerator :: duckdb_type_ubigint      = 9  ! uint64_t
    enumerator :: duckdb_type_float        = 10 ! float
    enumerator :: duckdb_type_double       = 11 ! double
    enumerator :: duckdb_type_timestamp    = 12 ! duckdb_timestamp, in microseconds
    enumerator :: duckdb_type_date         = 13 ! duckdb_date
    enumerator :: duckdb_type_time         = 14 ! duckdb_time
    enumerator :: duckdb_type_interval     = 15 ! duckdb_interval
    enumerator :: duckdb_type_hugeint      = 16 ! duckdb_hugeint
    enumerator :: duckdb_type_varchar      = 17 ! const char*
    enumerator :: duckdb_type_blob         = 18 ! duckdb_blob
    enumerator :: duckdb_type_decimal      = 19 ! decimal
    enumerator :: duckdb_type_timestamp_s  = 20 ! duckdb_timestamp, in seconds
    enumerator :: duckdb_type_timestamp_ms = 21 ! duckdb_timestamp, in milliseconds
    enumerator :: duckdb_type_timestamp_ns = 22 ! duckdb_timestamp, in nanoseconds
    enumerator :: duckdb_type_enum         = 23 ! enum type, only useful as logical type
    enumerator :: duckdb_type_list         = 24 ! list type, only useful as logical type
    enumerator :: duckdb_type_struct       = 25 ! struct type, only useful as logical type
    enumerator :: duckdb_type_map          = 26 ! map type, only useful as logical type
    enumerator :: duckdb_type_uuid         = 27 ! duckdb_hugeint
    enumerator :: duckdb_type_union        = 28 ! union type, only useful as logical type
    enumerator :: duckdb_type_bit          = 29 ! duckdb_bit
  end enum

  enum, bind(c)
    enumerator :: duckdb_pending_state = 0
    enumerator :: duckdb_pending_result_ready = 0
    enumerator :: duckdb_pending_result_not_ready = 1
    enumerator :: duckdb_pending_error = 2
  end enum

  type, bind(c) :: duckdb_date
    integer(kind=c_int32_t) :: days
  end type

  type, bind(c) :: duckdb_date_struct
    integer(kind=c_int32_t) :: year
    integer(kind=c_int8_t) :: month
    integer(kind=c_int8_t) :: day
  end type

  type, bind(c) :: duckdb_time
   integer(kind=c_int64_t) :: micros
  end type

  type, bind(c) :: duckdb_time_struct
    integer(kind=c_int8_t) :: hour
    integer(kind=c_int8_t) :: min
    integer(kind=c_int8_t) :: sec
    integer(kind=c_int32_t) :: micros
  end type

  type, bind(c) :: duckdb_timestamp
   integer(kind=c_int64_t) :: micros
  end type

  type, bind(c) :: duckdb_timestamp_struct
    type(duckdb_date_struct) :: date
    type(duckdb_time_struct) :: time
  end type

  type, bind(c) :: duckdb_interval
    integer(kind=c_int32_t) :: months
    integer(kind=c_int32_t) :: days
    integer(kind=c_int64_t) :: micros
  end type

  type, bind(c) :: duckdb_hugeint
    integer(kind=c_int64_t) :: lower
    integer(kind=c_int64_t) :: upper
  end type

  type, bind(c) :: duckdb_decimal
    integer(kind=c_int8_t) :: width
    integer(kind=c_int8_t) :: scale
    type(duckdb_hugeint) :: value
  end type

  type, bind(c) :: duckdb_string
    type(c_ptr) :: data
    integer(kind=c_int64_t) :: size
  end type

  type, bind(c) :: duckdb_blob
    type(c_ptr) :: data
    integer(kind=c_int64_t) :: size
  end type

  type, bind(c) :: duckdb_list_entry
    integer(kind=c_int64_t) :: offset
    integer(kind=c_int64_t) :: length
  end type

  type, bind(c) :: duckdb_column
    type(c_ptr) :: internal_data
  end type

  type, bind(c) :: duckdb_result
    type(c_ptr) :: internal_data
  end type

  interface

    ! DUCKDB_API duckdb_state duckdb_open(const char *path, duckdb_database *out_database);
    function duckdb_open(path, out_database)&
    bind(c, name='duckdb_open')&
    result(res)
      import :: duckdb_state, c_ptr
      integer(kind(duckdb_state)) :: res
      type(c_ptr), value :: path
      type(c_ptr) :: out_database
    end function duckdb_open

    ! DUCKDB_API void duckdb_close(duckdb_database *database);
    subroutine duckdb_close(database)&
    bind(c, name='duckdb_close')
      import :: c_ptr
      type(c_ptr) :: database
    end subroutine duckdb_close

    ! DUCKDB_API duckdb_state duckdb_connect(duckdb_database database, duckdb_connection *out_connection);
    function duckdb_connect(database, out_connection)&
    bind(c, name='duckdb_connect')&
    result(res)
      import :: duckdb_state, c_char, c_ptr
      integer(kind(duckdb_state)) :: res
      type(c_ptr), value :: database
      type(c_ptr) :: out_connection
    end function duckdb_connect

    ! DUCKDB_API void duckdb_disconnect(duckdb_connection *connection);
    subroutine duckdb_disconnect(connection)&
    bind(c, name='duckdb_disconnect')
      import :: c_ptr
      type(c_ptr) :: connection
    end subroutine duckdb_disconnect

    ! DUCKDB_API duckdb_state duckdb_query(duckdb_connection connection, const char *query, duckdb_result *out_result);
    function duckdb_query_(connection, query, out_result)&
    bind(c, name='duckdb_query')&
    result(res)
      import :: duckdb_state, duckdb_result, c_char, c_ptr
      integer(kind(duckdb_state)) :: res
      type(c_ptr), value :: connection
      character(kind=c_char) :: query ! must be a c string
      type(c_ptr), value :: out_result
    end function duckdb_query_

    ! DUCKDB_API void duckdb_destroy_result(duckdb_result *result);
    ! FIXME
    subroutine duckdb_destroy_result_(res)&
    bind(c, name='duckdb_destroy_result')
      import :: c_ptr
      type(c_ptr), value :: res
    end subroutine duckdb_destroy_result_

    ! DUCKDB_API idx_t duckdb_column_count(duckdb_result *result);
    function duckdb_column_count_(res) result(cc)&
    bind(c, name='duckdb_column_count')
      import :: c_ptr, c_int64_t
      type(c_ptr), value :: res
      integer(kind=c_int64_t) :: cc
    end function duckdb_column_count_

    ! DUCKDB_API idx_t duckdb_row_count(duckdb_result *result);
    function duckdb_row_count_(res) result(cc)&
    bind(c, name='duckdb_row_count')
      import :: c_ptr, c_int64_t
      type(c_ptr), value :: res
      integer(kind=c_int64_t) :: cc
    end function duckdb_row_count_

    ! DUCKDB_API idx_t duckdb_rows_changed(duckdb_result *result);
    function duckdb_rows_changed_(res) result(cc)&
    bind(c, name='duckdb_rows_changed')
      import :: c_ptr, c_int64_t
      type(c_ptr), value :: res
      integer(kind=c_int64_t) :: cc
    end function duckdb_rows_changed_

    ! DUCKDB_API const char *duckdb_library_version();
    function duckdb_library_version_()&
    bind(c, name='duckdb_library_version')&
    result(res)
      import :: c_ptr
      type(c_ptr) :: res
    end function duckdb_library_version_

    ! DUCKDB_API const char *duckdb_result_error(duckdb_result *result);
    function duckdb_result_error_(res) result(err)&
    bind(c, name='duckdb_result_error')
      import :: c_ptr
      type(c_ptr) :: err
      type(c_ptr), value :: res
    end function duckdb_result_error_

    ! DUCKDB_API const char *duckdb_column_name(duckdb_result *result, idx_t col);
    ! TODO: col is zero-based - do we want this to be one based for fortran?.
    function duckdb_column_name_(res, col)&
    bind(c, name='duckdb_column_name')&
    result(name)
      import :: c_ptr, c_int64_t
      type(c_ptr) :: name
      type(c_ptr), value :: res
      integer(kind=c_int64_t), value :: col
    end function duckdb_column_name_

    ! DUCKDB_API duckdb_type duckdb_column_type(duckdb_result *result, idx_t col);
    function duckdb_column_type_(res, col)&
    bind(c, name='duckdb_column_type')&
    result(col_type)
      import :: c_ptr, c_int64_t
      integer(kind(duckdb_type)) :: col_type
      type(c_ptr), value :: res
      integer(kind=c_int64_t), value :: col
    end function duckdb_column_type_

    ! DUCKDB_API void *duckdb_column_data(duckdb_result *result, idx_t col);
    function duckdb_column_data_(res, col)&
    bind(c, name='duckdb_column_data')&
    result(data)
      import :: c_ptr, c_int64_t
      type(c_ptr), value :: res 
      integer(kind=c_int64_t), value :: col
      type(c_ptr) :: data
    end function duckdb_column_data_

    ! DUCKDB_API bool *duckdb_nullmask_data(duckdb_result *result, idx_t col);
    function duckdb_nullmask_data_(res, col)&
    bind(c, name='duckdb_nullmask_data')&
    result(mask)
      import :: c_ptr, c_int64_t
      type(c_ptr), value :: res 
      integer(kind=c_int64_t), value :: col
      type(c_ptr) :: mask
    end function duckdb_nullmask_data_

    ! DUCKDB_API duckdb_data_chunk duckdb_result_get_chunk(duckdb_result result, idx_t chunk_index);
    ! FIXME
    function duckdb_result_get_chunk_(res, idx)&
    bind(c, name='duckdb_result_get_chunk')&
    result(chunk)
      import :: c_ptr, c_int64_t
      type(c_ptr), value :: res
      integer(kind=c_int64_t), value :: idx
      type(c_ptr) :: chunk
    end function duckdb_result_get_chunk_

    ! DUCKDB_API idx_t duckdb_result_chunk_count(duckdb_result result);
    ! FIXME
    function duckdb_result_chunk_count_(res)&
    bind(c, name='duckdb_result_chunk_count')&
    result(cc)
      import :: c_ptr, c_int64_t
      type(c_ptr), value :: res
      integer(kind=c_int64_t) :: cc
    end function duckdb_result_chunk_count_

    ! DUCKDB_API bool duckdb_value_boolean(duckdb_result *result, idx_t col, idx_t row);
    function duckdb_value_boolean_(res, col, row)&
    bind(c, name='duckdb_value_boolean')&
    result(r)
      import :: c_ptr, c_bool, c_int64_t
      type(c_ptr), value :: res
      integer(kind=c_int64_t), value :: col, row
      logical(kind=c_bool) :: r
    end function duckdb_value_boolean_

    ! DUCKDB_API int8_t duckdb_value_int8(duckdb_result *result, idx_t col, idx_t row);
    function duckdb_value_int8_(res, col, row)&
    bind(c, name='duckdb_value_int8')&
    result(r)
      import :: c_ptr, c_int8_t, c_int64_t
      type(c_ptr), value :: res
      integer(kind=c_int64_t), value :: col, row
      integer(kind=c_int8_t) :: r
    end function duckdb_value_int8_

    ! DUCKDB_API int16_t duckdb_value_int16(duckdb_result *result, idx_t col, idx_t row);
    function duckdb_value_int16_(res, col, row)&
    bind(c, name='duckdb_value_int16')&
    result(r)
      import :: c_ptr, c_int16_t, c_int64_t
      type(c_ptr), value :: res
      integer(kind=c_int64_t), value :: col, row
      integer(kind=c_int16_t) :: r
    end function duckdb_value_int16_

    ! DUCKDB_API int32_t duckdb_value_int32(duckdb_result *result, idx_t col, idx_t row);
    function duckdb_value_int32_(res, col, row)&
    bind(c, name='duckdb_value_int32')&
    result(r)
      import :: c_ptr, c_int32_t, c_int64_t
      type(c_ptr), value :: res
      integer(kind=c_int64_t), value :: col, row
      integer(kind=c_int32_t) :: r
    end function duckdb_value_int32_

    ! DUCKDB_API int64_t duckdb_value_int64(duckdb_result *result, idx_t col, idx_t row);
    function duckdb_value_int64_(res, col, row)&
    bind(c, name='duckdb_value_int64')&
    result(r)
      import :: c_ptr, c_int64_t
      type(c_ptr), value :: res
      integer(kind=c_int64_t), value :: col, row
      integer(kind=c_int64_t) :: r
    end function duckdb_value_int64_

    ! DUCKDB_API duckdb_hugeint duckdb_value_hugeint(duckdb_result *result, idx_t col, idx_t row);
    function duckdb_value_hugeint_(res, col, row)&
    bind(c, name='duckdb_value_hugeint')&
    result(r)
      import :: c_ptr, c_int64_t, duckdb_hugeint
      type(c_ptr), value :: res
      integer(kind=c_int64_t), value :: col, row
      type(duckdb_hugeint) :: r
    end function duckdb_value_hugeint_

    ! DUCKDB_API duckdb_decimal duckdb_value_decimal(duckdb_result *result, idx_t col, idx_t row);
    function duckdb_value_decimal_(res, col, row)&
    bind(c, name='duckdb_value_decimal')&
    result(r)
      import :: c_ptr, c_int64_t, duckdb_decimal
      type(c_ptr), value :: res
      integer(kind=c_int64_t), value :: col, row
      type(duckdb_decimal) :: r
    end function duckdb_value_decimal_

    ! TODO - Do we need these unsigned versions?
    ! DUCKDB_API uint8_t duckdb_value_uint8(duckdb_result *result, idx_t col, idx_t row);
    ! DUCKDB_API uint16_t duckdb_value_uint16(duckdb_result *result, idx_t col, idx_t row);
    ! DUCKDB_API uint32_t duckdb_value_uint32(duckdb_result *result, idx_t col, idx_t row);
    ! DUCKDB_API uint64_t duckdb_value_uint64(duckdb_result *result, idx_t col, idx_t row);

    ! DUCKDB_API float duckdb_value_float(duckdb_result *result, idx_t col, idx_t row);
    function duckdb_value_float_(res, col, row)&
    bind(c, name='duckdb_value_float')&
    result(r)
      import :: c_ptr, c_float, c_int64_t
      type(c_ptr), value :: res
      integer(kind=c_int64_t), value :: col, row
      real(kind=c_float) :: r
    end function duckdb_value_float_

    ! DUCKDB_API double duckdb_value_double(duckdb_result *result, idx_t col, idx_t row);
    function duckdb_value_double_(res, col, row)&
    bind(c, name='duckdb_value_double')&
    result(r)
      import :: c_ptr, c_double, c_int64_t
      type(c_ptr), value :: res
      integer(kind=c_int64_t), value :: col, row
      real(kind=c_double) :: r
    end function duckdb_value_double_

    ! DUCKDB_API duckdb_date duckdb_value_date(duckdb_result *result, idx_t col, idx_t row);
    function duckdb_value_date_(res, col, row)&
    bind(c, name='duckdb_value_date')&
    result(r)
      import :: c_ptr, c_int64_t, duckdb_date
      type(c_ptr), value :: res
      integer(kind=c_int64_t), value :: col, row
      type(duckdb_date) :: r
    end function duckdb_value_date_

    ! DUCKDB_API duckdb_time duckdb_value_time(duckdb_result *result, idx_t col, idx_t row);
    function duckdb_value_time_(res, col, row)&
    bind(c, name='duckdb_value_time')&
    result(r)
      import :: c_ptr, c_int64_t, duckdb_time
      type(c_ptr), value :: res
      integer(kind=c_int64_t), value :: col, row
      type(duckdb_time) :: r
    end function duckdb_value_time_

    ! DUCKDB_API duckdb_timestamp duckdb_value_timestamp(duckdb_result *result, idx_t col, idx_t row);
    function duckdb_value_timestamp_(res, col, row)&
    bind(c, name='duckdb_value_timestamp')&
    result(r)
      import :: c_ptr, c_int64_t, duckdb_timestamp
      type(c_ptr), value :: res
      integer(kind=c_int64_t), value :: col, row
      type(duckdb_timestamp) :: r
    end function duckdb_value_timestamp_

    ! DUCKDB_API duckdb_interval duckdb_value_interval(duckdb_result *result, idx_t col, idx_t row);
    function duckdb_value_interval_(res, col, row)&
    bind(c, name='duckdb_value_interval')&
    result(r)
      import :: c_ptr, c_int64_t, duckdb_interval
      type(c_ptr), value :: res
      integer(kind=c_int64_t), value :: col, row
      type(duckdb_interval) :: r
    end function duckdb_value_interval_

    ! DUCKDB_API duckdb_string duckdb_value_string(duckdb_result *result, idx_t col, idx_t row);
    function duckdb_value_string_(res, col, row)&
    bind(c, name='duckdb_value_string')&
    result(r)
      import :: c_ptr, c_int64_t, duckdb_string
      type(c_ptr), value :: res
      integer(kind=c_int64_t), value :: col, row
      type(duckdb_string) :: r
    end function duckdb_value_string_

    ! DUCKDB_API duckdb_blob duckdb_value_blob(duckdb_result *result, idx_t col, idx_t row);
    function duckdb_value_blob_(res, col, row)&
    bind(c, name='duckdb_value_blob')&
    result(r)
      import :: c_ptr, c_int64_t, duckdb_blob
      type(c_ptr), value :: res
      integer(kind=c_int64_t), value :: col, row
      type(duckdb_blob) :: r
    end function duckdb_value_blob_

    ! DUCKDB_API bool duckdb_value_is_null(duckdb_result *result, idx_t col, idx_t row);
    function duckdb_value_is_null_(res, col, row)&
    bind(c, name='duckdb_value_is_null')&
    result(r)
      import :: c_ptr, c_bool, c_int64_t
      type(c_ptr), value :: res
      integer(kind=c_int64_t), value :: col, row
      logical(kind=c_bool) :: r
    end function duckdb_value_is_null_

    ! DUCKDB_API void *duckdb_malloc(size_t size);
    function duckdb_malloc(size)&
    bind(c, name='duckdb_malloc')&
    result(res)
      import :: c_ptr, c_size_t
      integer(kind=c_size_t) :: size
      type(c_ptr) :: res
    end function duckdb_malloc

    ! DUCKDB_API void duckdb_free(void *ptr);
    subroutine duckdb_free(ptr)&
    bind(c, name='duckdb_free')
      import :: c_ptr
      type(c_ptr) :: ptr
    end subroutine duckdb_free

    ! DUCKDB_API idx_t duckdb_vector_size();
    function duckdb_vector_size()&
    bind(c, name='duckdb_vector_size')&
    result(res)
      import :: c_int64_t
      integer(kind=c_int64_t) :: res
    end function duckdb_vector_size

    ! DUCKDB_API duckdb_date_struct duckdb_from_date(duckdb_date date);
    function duckdb_from_date(date)&
    bind(c, name='duckdb_from_date')&
    result(res)
      import :: duckdb_date, duckdb_date_struct
      type(duckdb_date) :: date
      type(duckdb_date_struct) :: res
    end function duckdb_from_date

    ! DUCKDB_API duckdb_date duckdb_to_date(duckdb_date_struct date);
    function duckdb_to_date(date)&
    bind(c, name='duckdb_to_date')&
    result(res)
      import :: duckdb_date, duckdb_date_struct
      type(duckdb_date) :: res
      type(duckdb_date_struct) :: date
    end function duckdb_to_date

    ! DUCKDB_API duckdb_time_struct duckdb_from_time(duckdb_time time);
    function duckdb_from_time(time)&
    bind(c, name='duckdb_from_time')&
    result(res)
      import :: duckdb_time, duckdb_time_struct
      type(duckdb_time) :: time
      type(duckdb_time_struct) :: res
    end function duckdb_from_time

    ! DUCKDB_API duckdb_time duckdb_to_time(duckdb_time_struct time);
    function duckdb_to_time(time)&
    bind(c, name='duckdb_to_time')&
    result(res)
      import :: duckdb_time, duckdb_time_struct
      type(duckdb_time) :: res
      type(duckdb_time_struct) :: time
    end function duckdb_to_time

    ! DUCKDB_API duckdb_timestamp_struct duckdb_from_timestamp(duckdb_timestamp ts);
    function duckdb_from_timestamp(timestamp)&
    bind(c, name='duckdb_from_timestamp')&
    result(res)
      import :: duckdb_timestamp, duckdb_timestamp_struct
      type(duckdb_timestamp) :: timestamp
      type(duckdb_timestamp_struct) :: res
    end function duckdb_from_timestamp

    ! DUCKDB_API duckdb_timestamp duckdb_to_timestamp(duckdb_timestamp_struct ts);
    function duckdb_to_timestamp(timestamp)&
    bind(c, name='duckdb_to_timestamp')&
    result(res)
      import :: duckdb_timestamp, duckdb_timestamp_struct
      type(duckdb_timestamp) :: res
      type(duckdb_timestamp_struct) :: timestamp
    end function duckdb_to_timestamp

    ! DUCKDB_API double duckdb_hugeint_to_double(duckdb_hugeint val);
    function duckdb_hugeint_to_double(val)&
    bind(c, name='duckdb_hugeint_to_double')&
    result(res)
      import :: c_double, duckdb_hugeint
      type(duckdb_hugeint) :: val
      real(kind=c_double) :: res
    end function duckdb_hugeint_to_double

    ! DUCKDB_API duckdb_hugeint duckdb_double_to_hugeint(double val);
    function duckdb_double_to_hugeint(val)&
    bind(c, name='duckdb_double_to_hugeint')&
    result(res)
      import :: c_double, duckdb_hugeint
      type(duckdb_hugeint) :: res
      real(kind=c_double) :: val
    end function duckdb_double_to_hugeint

    ! DUCKDB_API duckdb_decimal duckdb_double_to_decimal(double val, uint8_t width, uint8_t scale);
    function duckdb_double_to_decimal(val)&
    bind(c, name='duckdb_double_to_decimal')&
    result(res)
      import :: c_double, duckdb_decimal
      type(duckdb_decimal) :: res
      real(kind=c_double) :: val
    end function duckdb_double_to_decimal

    ! DUCKDB_API double duckdb_decimal_to_double(duckdb_decimal val);
    function duckdb_decimal_to_double(val)&
    bind(c, name='duckdb_decimal_to_double')&
    result(res)
      import :: c_double, duckdb_decimal
      type(duckdb_decimal) :: val
      real(kind=c_double) :: res
    end function duckdb_decimal_to_double

  end interface

  contains

    function duckdb_query(connection, query, out_result) result(res)
      integer(kind(duckdb_state)) :: res
      type(c_ptr), value :: connection
      character(len=*) :: query
      character(len=:), allocatable :: sql
      type(duckdb_result), pointer :: out_result
      type(c_ptr) :: tmp
      sql = query // c_null_char ! convert to c string
      tmp = c_loc(out_result)
      res = duckdb_query_(connection, sql, tmp)
      call c_f_pointer(tmp, out_result)
    end function duckdb_query

    function duckdb_column_count(res) result(cc)
      type(duckdb_result), pointer :: res
      type(c_ptr) :: tmp
      integer :: cc
      tmp = c_loc(res)
      cc = int(duckdb_column_count_(tmp))
    end function duckdb_column_count

    function duckdb_row_count(res) result(cc)
      type(duckdb_result), pointer :: res
      type(c_ptr) :: tmp
      integer :: cc
      tmp = c_loc(res)
      cc = int(duckdb_row_count_(tmp))
    end function duckdb_row_count

    function duckdb_rows_changed(res) result(cc)
      type(duckdb_result), pointer :: res
      type(c_ptr) :: tmp
      integer :: cc
      tmp = c_loc(res)
      cc = int(duckdb_rows_changed_(tmp))
    end function duckdb_rows_changed

    function duckdb_library_version() result(res)
      character(len=:), allocatable :: res
      type(c_ptr) :: tmp
      tmp = duckdb_library_version_()
      call c_f_str_ptr(tmp, res)
    end function duckdb_library_version

    function duckdb_column_name(res, col) result(name)
      character(len=:), allocatable :: name
      type(c_ptr) :: tmp1, tmp2
      type(duckdb_result), pointer :: res
      integer, value :: col
      tmp2 = c_loc(res)
      tmp1 = duckdb_column_name_(tmp2, int(col, kind=c_int64_t))
      if (c_associated(tmp1)) then
        call c_f_str_ptr(tmp1, name)
      else
        name = "NULL"
      end if
    end function duckdb_column_name

    function duckdb_column_type(res, col) result(col_type)
      integer(kind(duckdb_type)) :: col_type
      type(c_ptr) :: tmp1, tmp2
      type(duckdb_result), pointer :: res
      integer, value :: col
      tmp2 = c_loc(res)
      col_type= duckdb_column_type_(tmp2, int(col, kind=c_int64_t))
    end function duckdb_column_type

    subroutine duckdb_destroy_result(res)
      type(duckdb_result), pointer :: res
      type(c_ptr) :: tmp
      tmp = c_loc(res)
      call duckdb_destroy_result_(tmp)
    end subroutine duckdb_destroy_result

    function duckdb_result_error(res) result(err)
      character(len=:), allocatable :: err
      type(c_ptr) :: tmp1, tmp2
      type(duckdb_result), pointer :: res
      tmp2 = c_loc(res)
      tmp1 = duckdb_result_error_(tmp2)
      if (c_associated(tmp1)) then
        call c_f_str_ptr(tmp1, err)
      else
        err= "NULL"
      end if
    end function duckdb_result_error

    function duckdb_column_data(res, col) result(data)
      type(duckdb_result), pointer :: res
      integer :: col
      type(c_ptr) :: tmp
      type(c_ptr) :: data
      tmp = c_loc(res)
      data = duckdb_column_data_(tmp, int(col, kind=c_int64_t))
    end function duckdb_column_data

    function duckdb_nullmask_data(res, col) result(mask)
      type(duckdb_result), pointer :: res
      integer :: col
      type(c_ptr) :: tmp
      type(c_ptr) :: mask
      tmp = c_loc(res)
      mask = duckdb_nullmask_data_(tmp, int(col, kind=c_int64_t))
    end function duckdb_nullmask_data

    ! FIXME
    function duckdb_result_get_chunk(res, idx) result(chunk)
      type(duckdb_result), pointer :: res
      type(c_ptr) :: tmp
      integer :: idx
      type(c_ptr) :: chunk
      tmp = c_loc(res)
      chunk = duckdb_result_get_chunk_(tmp, int(idx, kind=c_int64_t))
    end function duckdb_result_get_chunk

    ! FIXME
    function duckdb_result_chunk_count(res) result(cc)
      type(duckdb_result), pointer :: res
      type(c_ptr) :: tmp
      integer :: cc
      tmp = c_loc(res)
      cc = int(duckdb_result_chunk_count_(tmp))
    end function duckdb_result_chunk_count

    function duckdb_value_boolean(res, col, row) result(r)
      type(duckdb_result), pointer :: res
      type(c_ptr) :: tmp
      integer :: col, row
      logical :: r
      tmp = c_loc(res)
      r = duckdb_value_boolean_(tmp, int(col, kind=c_int64_t), int(row, kind=c_int64_t))
    end function duckdb_value_boolean

    function duckdb_value_int8(res, col, row) result(r)
      type(duckdb_result), pointer :: res
      type(c_ptr) :: tmp
      integer :: col, row
      integer(kind=int8) :: r
      tmp = c_loc(res)
      r = int(duckdb_value_int8_(tmp, int(col, kind=c_int64_t), int(row, kind=c_int64_t)), kind=int8)
    end function duckdb_value_int8

    function duckdb_value_int16(res, col, row) result(r)
      type(duckdb_result), pointer :: res
      type(c_ptr) :: tmp
      integer :: col, row
      integer(kind=int16) :: r
      tmp = c_loc(res)
      r = int(duckdb_value_int16_(tmp, int(col, kind=c_int64_t), int(row, kind=c_int64_t)), kind=int16)
    end function duckdb_value_int16

    function duckdb_value_int32(res, col, row) result(r)
      type(duckdb_result), pointer :: res
      type(c_ptr) :: tmp
      integer :: col, row
      integer(kind=int32) :: r
      tmp = c_loc(res)
      r = int(duckdb_value_int32_(tmp, int(col, kind=c_int64_t), int(row, kind=c_int64_t)), kind=int32)
    end function duckdb_value_int32

    function duckdb_value_int64(res, col, row) result(r)
      type(duckdb_result), pointer :: res
      type(c_ptr) :: tmp
      integer :: col, row
      integer(kind=int64) :: r
      tmp = c_loc(res)
      r = int(duckdb_value_int64_(tmp, int(col, kind=c_int64_t), int(row, kind=c_int64_t)), kind=int64)
    end function duckdb_value_int64

    function duckdb_value_float(res, col, row) result(r)
      type(duckdb_result), pointer :: res
      type(c_ptr) :: tmp
      integer :: col, row
      real(kind=real32) :: r
      tmp = c_loc(res)
      r = real(duckdb_value_float_(tmp, int(col, kind=c_int64_t), int(row, kind=c_int64_t)), kind=real32)
    end function duckdb_value_float

    function duckdb_value_double(res, col, row) result(r)
      type(duckdb_result), pointer :: res
      type(c_ptr) :: tmp
      integer :: col, row
      real(kind=real64) :: r
      tmp = c_loc(res)
      r = real(duckdb_value_double_(tmp, int(col, kind=c_int64_t), int(row, kind=c_int64_t)), kind=real64)
    end function duckdb_value_double

    function duckdb_value_date(res, col, row) result(r)
      type(duckdb_result), pointer :: res
      type(c_ptr) :: tmp
      integer :: col, row
      type(duckdb_date) :: r
      tmp = c_loc(res)
      r = duckdb_value_date_(tmp, int(col, kind=c_int64_t), int(row, kind=c_int64_t))
    end function duckdb_value_date

    function duckdb_value_time(res, col, row) result(r)
      type(duckdb_result), pointer :: res
      type(c_ptr) :: tmp
      integer :: col, row
      type(duckdb_time) :: r
      tmp = c_loc(res)
      r = duckdb_value_time_(tmp, int(col, kind=c_int64_t), int(row, kind=c_int64_t))
    end function duckdb_value_time

    function duckdb_value_timestamp(res, col, row) result(r)
      type(duckdb_result), pointer :: res
      type(c_ptr) :: tmp
      integer :: col, row
      type(duckdb_timestamp) :: r
      tmp = c_loc(res)
      r = duckdb_value_timestamp_(tmp, int(col, kind=c_int64_t), int(row, kind=c_int64_t))
    end function duckdb_value_timestamp

    function duckdb_value_interval(res, col, row) result(r)
      type(duckdb_result), pointer :: res
      type(c_ptr) :: tmp
      integer :: col, row
      type(duckdb_interval) :: r
      tmp = c_loc(res)
      r = duckdb_value_interval_(tmp, int(col, kind=c_int64_t), int(row, kind=c_int64_t))
    end function duckdb_value_interval

    function duckdb_value_string(res, col, row) result(r)
      type(duckdb_result), pointer :: res
      type(c_ptr) :: tmp
      integer :: col, row
      type(duckdb_string) :: r
      tmp = c_loc(res)
      r = duckdb_value_string_(tmp, int(col, kind=c_int64_t), int(row, kind=c_int64_t))
    end function duckdb_value_string

    function duckdb_value_blob(res, col, row) result(r)
      type(duckdb_result), pointer :: res
      type(c_ptr) :: tmp
      integer :: col, row
      type(duckdb_blob) :: r
      tmp = c_loc(res)
      r = duckdb_value_blob_(tmp, int(col, kind=c_int64_t), int(row, kind=c_int64_t))
    end function duckdb_value_blob

    function duckdb_value_is_null(res, col, row) result(r)
      type(duckdb_result), pointer :: res
      type(c_ptr) :: tmp
      integer :: col, row
      logical :: r
      tmp = c_loc(res)
      r = duckdb_value_is_null_(tmp, int(col, kind=c_int64_t), int(row, kind=c_int64_t))
    end function duckdb_value_is_null

end module duckdb
