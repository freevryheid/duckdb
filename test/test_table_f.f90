module test_table_f

  use, intrinsic :: iso_c_binding, only: c_int64_t, c_ptr, c_loc, c_funloc, c_f_pointer, c_null_ptr, c_int
  use constants
  use duckdb
  use testdrive, only: new_unittest, unittest_type, error_type, check, skip_test
  implicit none
  private
  public :: collect_table_f

  ! type, bind(c) :: my_bind_data_struct
  !   integer(kind=c_int64_t) :: size
  ! end type

  ! type, bind(c) :: my_init_data_struct
  !   integer(kind=c_int64_t) :: pos
  ! end type

  type :: my_bind_data_struct
    integer(kind=c_int64_t) :: size
  end type

  type :: my_init_data_struct
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

    ! subroutine dummy_free(ptr) bind(c)
    !   type(c_ptr), value :: ptr
    !   print*, 'Skipping free for', ptr
    ! end subroutine dummy_free

    subroutine dummy_free(ptr)
      type(c_ptr), value :: ptr
      print*, 'Skipping free for', ptr
    end subroutine dummy_free

    ! subroutine my_bind(info) bind(c)
    subroutine my_bind(info)
      type(c_ptr), value :: info

      type(duckdb_logical_type) :: type
      ! type(my_bind_data_struct), pointer :: my_bind_data
      type(my_bind_data_struct), target:: my_bind_data
      type(duckdb_value) :: param
      ! procedure(duckdb_delete_callback_t), pointer :: func_ptr

      print*, "Start my_bind"
      if (duckdb_bind_get_parameter_count(info) /= 1) then
        error stop "Error in getting param count"
      endif

      type = duckdb_create_logical_type(duckdb_type_bigint)
      call duckdb_bind_add_result_column(info, "forty_two", type)
      call duckdb_destroy_logical_type(type)

      param = duckdb_bind_get_parameter(info, 0)
      print*, "  Retrieved size value from param: ", duckdb_get_int64(param)
      ! allocate(my_bind_data)
      my_bind_data%size = duckdb_get_int64(param)
      call duckdb_destroy_value(param)

      ! func_ptr => c_free
      ! func_ptr => dummy_free
      ! call duckdb_bind_set_bind_data(info, c_loc(my_bind_data), func_ptr)
      call duckdb_bind_set_bind_data(info, c_loc(my_bind_data), c_funloc(dummy_free))
      print*, "  Set the bind size to ", my_bind_data%size
      ! deallocate(my_bind_data)
      print*, "Done my_bind"
    end subroutine my_bind

    ! subroutine my_init(info) bind(c)
    subroutine my_init(info)
      type(c_ptr), value :: info
      ! type(my_init_data_struct), pointer :: my_init_data
      type(my_init_data_struct), target :: my_init_data
      type(c_ptr) :: ptr
      ! procedure(duckdb_delete_callback_t), pointer :: func_ptr
      print*, "Start my_init"
      ! allocate(my_init_data)
      my_init_data%pos = 0_int64
      ! func_ptr => c_free
      ! func_ptr => dummy_free
      ! call duckdb_init_set_init_data(info, c_loc(my_init_data), func_ptr)
      call duckdb_init_set_init_data(info, c_loc(my_init_data), c_funloc(dummy_free))
      print*, "  Set the init pos to ", my_init_data%pos
      ! deallocate(my_init_data)
      print*, "Done my_init"
    end subroutine my_init

    ! subroutine my_function(info, output) bind(c)
    subroutine my_function(info, output)
      type(c_ptr), value :: info
      type(duckdb_data_chunk), value :: output
      type(my_bind_data_struct), pointer :: bind_data
      type(my_init_data_struct), pointer :: init_data
      type(c_ptr) :: tmp
      integer(kind=c_int), pointer :: v(:)
      integer :: i

      print*, "Starting execution of my_function"
      tmp = duckdb_function_get_bind_data(info)
      call c_f_pointer(tmp, bind_data)
      print*, "  Retrieved bind size: ", bind_data%size
      tmp = duckdb_function_get_init_data(info)
      call c_f_pointer(tmp, init_data)
      print*, "  Retrieved init pos: ", init_data%pos
      tmp = duckdb_vector_get_data(duckdb_data_chunk_get_vector(output, 0))
      ! print *, "tmp: ", tmp
      print*, "  Size of output: ", duckdb_data_chunk_get_size(output)
      ! call c_f_pointer(tmp, v, [duckdb_data_chunk_get_size(output)])
      call c_f_pointer(tmp, v, [STANDARD_VECTOR_SIZE])

      ! do i = 1, STANDARD_VECTOR_SIZE
      do i = 1, 3
        ! print*, i, STANDARD_VECTOR_SIZE
        if (init_data%pos >= bind_data%size) exit
        if (mod(init_data%pos, 2) == 0) then
          v(i) = 42
        else
          v(i) = 84
        end if
        init_data%pos = init_data%pos + 1
      end do
      call duckdb_data_chunk_set_size(output, i-1)
      print*, "Done executing my_function"
    end subroutine my_function

    subroutine capi_register_table_function(con, name, bind, init, f)
      type(duckdb_connection) :: con
      character(len=*) :: name
      type(c_ptr), value :: bind
      type(c_ptr), value :: init
      type(c_ptr), value :: f

      ! procedure(duckdb_table_function_bind_t) :: bind
      ! procedure(duckdb_table_function_init_t) :: init
      ! procedure(duckdb_table_function_t) :: f

      type(c_ptr) :: func
      integer(kind(duckdb_state)) :: status
      type(duckdb_logical_type) :: type

      print*, "Starting registering table function: ",trim(name)
      ! create a table function
      func = duckdb_create_table_function()
      call duckdb_table_function_set_name(c_null_ptr, name)
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
      call duckdb_destroy_table_function(c_null_ptr)
      print*, "Done registering table function: ", trim(name)
    end subroutine capi_register_table_function

    subroutine test_table_functions(error)
      type(error_type), allocatable, intent(out) :: error
      type(duckdb_database) :: database
      type(duckdb_connection) :: connection
      type(duckdb_result) :: result
      type(c_ptr) :: func
      integer(kind(duckdb_state)) :: status
      type(duckdb_logical_type) :: type

      call check(error, duckdb_open("", database) == duckdbsuccess)
      if (allocated(error)) return
      call check(error, duckdb_connect(database, connection) == duckdbsuccess)
      if (allocated(error)) return

      print*, "Step 1: Register a table function using callbacks for bind, init and function."
      call capi_register_table_function(connection, "my_function", c_funloc(my_bind), c_funloc(my_init), c_funloc(my_function))
      ! now call it
      print*, "Step 2: Run a query on my_function which should execute the callback functions."
      status = duckdb_query(connection, "SELECT * FROM my_function(1)", result)
      call check(error, status == duckdbsuccess, "could not run query.")
      if (allocated(error)) return
      call check(error, duckdb_value_int64(result, 0, 0) == 42, "could not match result.")
      if (allocated(error)) return
    end subroutine test_table_functions

end module test_table_f
