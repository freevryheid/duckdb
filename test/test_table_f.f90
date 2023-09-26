module test_table_f
  use, intrinsic :: iso_c_binding, only: c_int64_t, c_ptr, c_loc, c_f_pointer
  use constants
  use duckdb
  use testdrive, only: new_unittest, unittest_type, error_type, check, skip_test
  implicit none
  private
  public :: collect_table_f

  type, bind(c) :: my_bind_data_struct
    integer(kind=c_int64_t) :: size
  end type

  type, bind(c) :: my_init_data_struct
    integer(kind=c_int64_t) :: pos
  end type

  interface 
    subroutine c_free(ptr) bind(c,name="free")
      import :: c_ptr
      type(c_ptr), value :: ptr
    end subroutine c_free
  end interface 
contains
  subroutine collect_table_f(testsuite)
    type(unittest_type), allocatable, intent(out) :: testsuite(:)
    testsuite = [                                                               &
      new_unittest("table-functions-test", test_table_functions)          &
    ]
  end subroutine collect_table_f

  subroutine my_bind(info) bind(c)
    type(duckdb_bind_info), value :: info
    type(duckdb_logical_type) :: type
    type(my_bind_data_struct), target :: my_bind_data
    type(duckdb_value) :: param
    procedure(duckdb_delete_callback_t), pointer :: func_ptr

    if (duckdb_bind_get_parameter_count(info) /= 1) then 
      write(*, *) "Error in getting param count"
    endif

    type = duckdb_create_logical_type(duckdb_type_bigint)
    call duckdb_bind_add_result_column(info, "forty_two", type)
    call duckdb_destroy_logical_type(type)

    param = duckdb_bind_get_parameter(info, 0)
    my_bind_data%size = duckdb_get_int64(param)
    call duckdb_destroy_value(param)

    func_ptr => c_free
    call duckdb_bind_set_bind_data(info, c_loc(my_bind_data), func_ptr)
  end subroutine my_bind

  subroutine my_init(info) bind(c)
    type(duckdb_init_info), value :: info
    type(my_init_data_struct), target :: my_init_data
    procedure(duckdb_delete_callback_t), pointer :: func_ptr
    my_init_data%pos = 0
    func_ptr => c_free
    call duckdb_init_set_init_data(info, c_loc(my_init_data), func_ptr)
  end subroutine my_init

  subroutine my_function(info, output) bind(c)
    type(duckdb_function_info), value :: info
    type(duckdb_data_chunk), value :: output
    type(my_bind_data_struct), pointer :: bind_data
    type(my_init_data_struct), pointer :: init_data
    type(c_ptr) :: tmp
    integer(kind=c_int64_t), pointer :: v(:)
    integer :: i

    tmp = duckdb_function_get_bind_data(info)
    call c_f_pointer(tmp, bind_data)
    tmp = duckdb_function_get_init_data(info)
    call c_f_pointer(tmp, bind_data)
    tmp = duckdb_vector_get_data(duckdb_data_chunk_get_vector(output, 0))
    call c_f_pointer(tmp, v, [duckdb_data_chunk_get_size(output)])
    do i = 1, STANDARD_VECTOR_SIZE
      if (init_data%pos >= bind_data%size) exit
      if (mod(init_data%pos, 2) == 0) then 
        v(i) = 42
      else 
        v(i) = 84
      end if
      init_data%pos = init_data%pos + 1
    end do
    call duckdb_data_chunk_set_size(output, i-1)
  end subroutine my_function

  subroutine capi_register_table_function(con, name, bind, init, f)
    type(duckdb_connection) :: con
    character(len=*) :: name
    procedure(duckdb_table_function_bind_t), pointer :: bind
    procedure(duckdb_table_function_init_t), pointer :: init
    procedure(duckdb_table_function_t), pointer :: f

    type(duckdb_table_function) :: func, func_uninit
    integer(kind(duckdb_state)) :: status
    type(duckdb_logical_type) :: type

    ! create a table function
    func = duckdb_create_table_function()
    call duckdb_table_function_set_name(func_uninit, name)
    call duckdb_table_function_set_name(func, "")
    call duckdb_table_function_set_name(func, name)
    call duckdb_table_function_set_name(func, name)

    ! add a string parameter
    type = duckdb_create_logical_type(duckdb_type_bigint)
    call duckdb_table_function_add_parameter(func, type)
    call duckdb_destroy_logical_type(type)

    ! add a named parameter
    type = duckdb_create_logical_type(duckdb_type_bigint)
    call duckdb_table_function_add_named_parameter(func, "my_parameter", type)
    call duckdb_destroy_logical_type(type)

    ! set up the function pointers
    call duckdb_table_function_set_bind(func, bind)
    call duckdb_table_function_set_init(func, init)
    call duckdb_table_function_set_function(func, f)

    ! register and cleanup
    status = duckdb_register_table_function(con, func)
    call duckdb_destroy_table_function(func)
    call duckdb_destroy_table_function(func)
    call duckdb_destroy_table_function(func_uninit)    
  end subroutine capi_register_table_function

  subroutine test_table_functions(error)
    type(error_type), allocatable, intent(out) :: error
    type(duckdb_database) :: database
    type(duckdb_connection) :: connection
    type(duckdb_result) :: result
    type(duckdb_table_function) :: func, func_uninit
    integer(kind(duckdb_state)) :: status
    type(duckdb_logical_type) :: type
    procedure(duckdb_table_function_bind_t), pointer :: bind_ptr
    procedure(duckdb_table_function_init_t), pointer :: init_ptr
    procedure(duckdb_table_function_t), pointer :: func_ptr

    call check(error, duckdb_open("", database) == duckdbsuccess)
    if (allocated(error)) return
    call check(error, duckdb_connect(database, connection) == duckdbsuccess)
    if (allocated(error)) return

    bind_ptr => my_bind
    init_ptr => my_init
    func_ptr => my_function
    call capi_register_table_function(connection, "my_function", bind_ptr, init_ptr, func_ptr)

    print*, "done register"
    ! now call it
    status = duckdb_query(connection, "SELECT * FROM my_function(1)", result)
    call check(error, status == duckdbsuccess, "could not run query.")
    if (allocated(error)) return
    call check(error, duckdb_value_int64(result, 0, 0) == 42, "could not match result.")
    if (allocated(error)) return

    !   ! create a table function
    ! func = duckdb_create_table_function()

    ! call duckdb_table_function_set_name(func_uninit, "my_function")
    ! call duckdb_table_function_set_name(func, "")
    ! call duckdb_table_function_set_name(func, "my_function")
    ! call duckdb_table_function_set_name(func, "my_function")

    ! ! add a string parameter
    ! type = duckdb_create_logical_type(duckdb_type_bigint)
    ! call duckdb_table_function_add_parameter(func, type)
    ! call duckdb_destroy_logical_type(type)

    ! ! add a named parameter
    ! type = duckdb_create_logical_type(duckdb_type_bigint)
    ! call duckdb_table_function_add_named_parameter(func, "my_parameter", type)
    ! call duckdb_destroy_logical_type(type)

    ! ! set up the function pointers
    ! bind_ptr => my_bind
    ! call duckdb_table_function_set_bind(func, bind_ptr)
    ! init_ptr => my_init
    ! call duckdb_table_function_set_init(func, init_ptr)
    ! func_ptr => my_function
    ! call duckdb_table_function_set_function(func, func_ptr)

    ! ! register and cleanup

    ! status = duckdb_register_table_function(connection, func)
    ! call check(error, status == duckdbsuccess, "error registering table function.")
    ! if (allocated(error)) return

    ! call duckdb_destroy_table_function(func)
    ! call duckdb_destroy_table_function(func)
    ! call duckdb_destroy_table_function(func_uninit)
  end subroutine test_table_functions
end module test_table_f
