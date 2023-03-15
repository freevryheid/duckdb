module duckdb
  use, intrinsic :: iso_c_binding
  use util
  implicit none
  private
  public :: duckdb_type, duckdb_type_invalid, duckdb_type_boolean, duckdb_type_tinyint
  public :: duckdb_type_smallint, duckdb_type_integer, duckdb_type_bigint, duckdb_type_utinyint
  public :: duckdb_type_usmallint, duckdb_type_uinteger, duckdb_type_ubigint, duckdb_type_float
  public :: duckdb_type_double, duckdb_type_timestamp, duckdb_type_date, duckdb_type_time
  public :: duckdb_type_interval, duckdb_type_hugeint, duckdb_type_varchar, duckdb_type_blob
  public :: duckdb_type_decimal, duckdb_type_timestamp_s, duckdb_type_timestamp_ms
  public :: duckdb_type_timestamp_ns, duckdb_type_enum, duckdb_type_list, duckdb_type_struct
  public :: duckdb_type_map, duckdb_type_uuid, duckdb_type_union, duckdb_type_bit
  public :: duckdb_column, duckdb_result
  public :: duckdb_state, duckdbsuccess, duckdberror
  public :: duckdb_open, duckdb_close
  public :: duckdb_connect, duckdb_disconnect
  public :: duckdb_query
  ! public :: duckdb_destroy_result
  public :: duckdb_column_count, duckdb_row_count
  public :: duckdb_library_version

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

  type, bind(c) :: duckdb_column
    type(c_ptr) :: data
    logical :: nullmask
    integer(kind(duckdb_type)) :: type
    character(kind=c_char) :: name
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
    ! TODO : needed? - if defined in fortran then destroy it in fortran
    ! subroutine duckdb_destroy_result(result1)&
    ! bind(c, name='duckdb_destroy_result')
    !   import :: c_ptr
    !   type(c_ptr) :: result1
    ! end subroutine duckdb_destroy_result

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

    ! DUCKDB_API const char *duckdb_library_version();
    function duckdb_library_version_()&
    bind(c, name='duckdb_library_version')&
    result(res)
      import :: c_ptr
      type(c_ptr) :: res
    end function duckdb_library_version_

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
      print*, "here_in"
      tmp = c_loc(out_result)
      res = duckdb_query_(connection, sql, tmp)
      call c_f_pointer(tmp, out_result)
      print*, "here_out"
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

    function duckdb_library_version()&
    result(res)
      character(len=:), allocatable :: res
      type(c_ptr) :: tmp
      tmp = duckdb_library_version_()
      call c_f_str_ptr(tmp, res)
    end function duckdb_library_version

end module duckdb
