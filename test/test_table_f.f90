module test_table_f
  use, intrinsic :: iso_c_binding, only: c_int64_t, c_ptr, c_loc
  use constants
  use duckdb
  use testdrive, only: new_unittest, unittest_type, error_type, check, skip_test
  implicit none
  private
  public :: collect_table_f

  type, bind(c) :: my_bind_data_struct
    integer(kind=c_int64_t) :: size
  end type
contains
  subroutine collect_table_f(testsuite)
    type(unittest_type), allocatable, intent(out) :: testsuite(:)
    testsuite = [                                                               &
      new_unittest("table-functions-test", test_table_functions)          &
    ]
  end subroutine collect_table_f


  subroutine my_bind(error, info)
    type(error_type), allocatable, intent(inout) :: error
    type(duckdb_bind_info) :: info
    type(duckdb_logical_type) :: type
    type(my_bind_data_struct) :: my_bind_data
    type(duckdb_value) :: param
    procedure(duckdb_delete_callback_t_interface), pointer :: func_ptr

    call check(error, duckdb_bind_get_parameter_count(info) == 1, "error in getting param count.")
    if (allocated(error)) return

    type = duckdb_create_logical_type(duckdb_type_bigint)
    call duckdb_bind_add_result_column(info, "forty_two", type)
    call duckdb_destroy_logical_type(type)

    param = duckdb_bind_get_parameter(info, 0)
    my_bind_data%size = duckdb_get_int64(param)
    call duckdb_destroy_value(param)

    func_ptr => my_free
    call duckdb_bind_set_bind_data(info, my_bind_data, func_ptr)
  end subroutine my_bind

  subroutine my_free(data)
    type(c_ptr), value :: data
    call free(c_loc(data))
  end subroutine

  subroutine test_table_functions(error)
    type(error_type), allocatable, intent(out) :: error
    type(duckdb_database) :: database
    type(duckdb_connection) :: connection
    type(duckdb_table_function) :: func, func_uninit
    integer(kind(duckdb_state)) :: status
    type(duckdb_logical_type) :: type
    ! integer :: size, i
    ! type(duckdb_pending_result) :: res
    ! type(duckdb_result) :: res2
    
    ! create a table function
    func = duckdb_create_table_function()

    call duckdb_table_function_set_name(func_uninit, "my_function")
    call duckdb_table_function_set_name(func, "")
    call duckdb_table_function_set_name(func, "my_function")
    call duckdb_table_function_set_name(func, "my_function")

    ! add a string parameter
    type = duckdb_create_logical_type(duckdb_type_bigint)
    call duckdb_table_function_add_parameter(func, type)
    call duckdb_destroy_logical_type(type)

    ! add a named parameter
    type = duckdb_create_logical_type(duckdb_type_bigint)
    call duckdb_table_function_add_named_parameter(func, "my_parameter", type)
    call duckdb_destroy_logical_type(type)

    ! set up the function pointers
    ! call duckdb_table_function_set_bind

    ! call check(error, duckdb_open("", database) == duckdbsuccess, "error opening db.")
    ! if (allocated(error)) return
    ! call check(error, duckdb_connect(database, connection) == duckdbsuccess, "error connecting.")
    ! if (allocated(error)) return
    ! status = duckdb_prepare(connection, "SELECT SUM(i) FROM range(1000000) tbl(i)", stmt)
    ! call check(error, status == duckdbsuccess, "error preparing query.")
    ! if (allocated(error)) return

    ! status = duckdb_pending_prepared(stmt, res)
    ! call check(error, status == duckdbsuccess, "error with pending prepared.")
    ! if (allocated(error)) return

    ! do
    !   call check(error, c_associated(stmt%prep), "pending statement not associated.")
    !   if (allocated(error)) return
    !   status = duckdb_pending_execute_task(res)
    !   call check(error, status /= duckdb_pending_error_state, "execute task error.")
    !   if (allocated(error)) return
    !   if (status == DUCKDB_PENDING_RESULT_READY) exit
    ! enddo      

    ! success = duckdb_execute_pending(res, res2)
    ! call check(error, success == duckdbsuccess, "error in execute pending.")
    ! if (allocated(error)) return

    ! call check(error, duckdb_value_int64(res2, 0, 0) == 499999500000_int64, "error in matching result pending.")
    ! if (allocated(error)) return

    ! call duckdb_destroy_result(res2)
    ! call duckdb_destroy_prepare(stmt)
    ! call duckdb_destroy_pending(res)
  end subroutine test_table_functions
end module test_table_f
