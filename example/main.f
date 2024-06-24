program main
  use, intrinsic :: iso_fortran_env
  use, intrinsic :: iso_c_binding
  use stdlib_linalg, only: is_square
  use duckdb
  use utils
  implicit none
  type(duckdb_database) :: db
  type(duckdb_connection) :: con
  integer(kind(duckdb_state)) :: r
  type(duckdb_result) :: res
  type(duckdb_data_chunk) :: chk
  integer(kind=int64) :: nc
  type(vectors), allocatable, dimension(:) :: vecs
  integer(kind=int64), pointer, dimension(:,:) :: mat ! 2d array
  integer :: i, j, k

  if (duckdb_open(db=db) == duckdberror) then
    error stop "open error"
  end if

  if (duckdb_connect(db, con) == duckdberror) then
    call duckdb_close(db)
    error stop "connect error"
  end if

  r = read_csv(con, "../a.csv", res)
  if (r == duckdberror) then
    call duckdb_disconnect(con)
    call duckdb_close(db)
    error stop "read error"
  end if

  nc = duckdb_result_chunk_count(res)
  if (nc > 1_int64) then
    call duckdb_destroy_result(res)
    call duckdb_disconnect(con)
    call duckdb_close(db)
    error stop "more than 1 data chunk - ..."
  end if

  chk = duckdb_result_get_chunk(res, 0_int64)
  call chk2vecs(chk, vecs)

    ! do k = 1, size(vecs)
    ! !   ! do k2 = 1, rows
    !     print *, vecs(k)%ptr
    !     print *, "---"
    ! !   ! end do
    ! end do

  ! do this before the chunk is destroyed
  if (allocated(vecs)) then
    allocate(mat(3,3))

    do i = 1, 3
      mat(1:3, i) = vecs(i)%ptr
    end do


    ! do i = 1, 3
    !   do j = 1,3
    !     print *, mat(i,j)
    !   end do
    ! end do

    print *, mat

    print *, is_square(mat)
    print *, matmul(mat,mat)


    deallocate(mat)



 else
    call duckdb_destroy_data_chunk(chk)
    call duckdb_destroy_result(res)
    call duckdb_disconnect(con)
    call duckdb_close(db)
    error stop "cannot allocate mat"
  end if

  deallocate(vecs)

  call duckdb_destroy_data_chunk(chk)
  call duckdb_destroy_result(res)
  call duckdb_disconnect(con)
  call duckdb_close(db)

end program main
