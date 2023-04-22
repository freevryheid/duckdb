program tester
  use, intrinsic :: iso_fortran_env, only : error_unit
  use testdrive, only : run_testsuite, new_testsuite, testsuite_type
  use test_starting_database, only : collect_starting_database
  use test_fortran_api, only: collect_fortran_api
  use test_parquet_files, only: collect_parquet_files
  use test_data_chunk, only: collect_data_chunk
  implicit none
  integer :: stat, is
  type(testsuite_type), allocatable :: testsuites(:)
  character(len=*), parameter :: fmt = '("#", *(1x, a))'

  stat = 0

  testsuites = [  &
    new_testsuite("starting_database", collect_starting_database), &
    new_testsuite("test_fortran_api", collect_fortran_api),        &
    new_testsuite("test_parquet_files", collect_parquet_files),    &
    new_testsuite("test_data_chunk", collect_data_chunk)           &
    ]

  do is = 1, size(testsuites)
    write(error_unit, fmt) "Testing:", testsuites(is)%name
    call run_testsuite(testsuites(is)%collect, error_unit, stat)
  end do

  if (stat > 0) then
    write(error_unit, '(i0, 1x, a)') stat, "test(s) failed!"
    error stop
  end if

end program tester
