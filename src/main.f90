program main
  use :: duckdb
  implicit none
  type(duckdb_database) :: db
  type(duckdb_connection) :: con
  ! type(dbr) :: res
  ! type(dbdc) :: chk
  ! type(dbv) :: vec

  ! integer :: r
  ! character(len=:), allocatable :: sql
  ! integer(kind=c_int64_t) :: i, j, nc, sc, cc
  ! type(c_ptr) :: va, vb
  ! real, pointer :: vr(:)

  if (duckdb_open(db=db) == 1) then
    print *, "error"
  end if

  if (duckdb_connect(db, con) == 1) then
    print *, "error"
  end if

  ! res = read_csv(con, "../data/attr.csv", "attr")
  ! ! if (r == 1) then
  ! !   print *, "error"
  ! ! end if

  ! sql = "select ria_rte_id, frm_dfo, to_dfo from attr;" // c_null_char
  ! r = db_query(con, sql, res)
  ! if (r == 1) then
  !   print *, "error"
  ! end if

  ! nc = db_result_chunk_count(res)
  ! do i = 0, nc - 1
  !   chk = db_result_get_chunk(res, i)
  !   sc = db_data_chunk_get_size(chk)
  !   cc = db_data_chunk_get_column_count(chk)
  !   vec = db_data_chunk_get_vector(chk, 1_c_int64_t)
  !   va = db_vector_get_data(vec)
  !   vb = db_vector_get_validity(vec)
  !   call c_f_pointer(va, vr, [sc])
  !   do j = 0, sc - 1
  !     ! if (db_validity_row_is_valid(vb, j)) then
  !     !   print *, vr(j+1)
  !     ! else
  !     !   print *, "NULL"
  !     ! end if
  !   end do
  !   print *, i, j
  !   call db_destroy_data_chunk(chk)
  ! end do

  ! deallocate(sql)
  ! call db_destroy_result(res)

  call duckdb_disconnect(con)
  call duckdb_close(db)

end program main
