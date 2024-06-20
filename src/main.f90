program main
  use, intrinsic :: iso_fortran_env
  use, intrinsic :: iso_c_binding
  use :: duckdb
  implicit none
  type(duckdb_database) :: db
  type(duckdb_connection) :: con
  integer(kind(duckdb_state)) :: r
  type(duckdb_result) :: res
  type(duckdb_data_chunk) :: chk
  type(duckdb_vector) :: vec
  integer(kind=int64) :: i, j, nc, sc, cc
  type(c_ptr) :: va, vb
  real, pointer :: vr(:)

  if (duckdb_open(db=db) == duckdberror) then
    error stop "open error"
  end if

  if (duckdb_connect(db, con) == duckdberror) then
    call duckdb_close(db)
    error stop "connect error"
  end if

  r = read_csv(con, "../data/attr.csv.gz", res)
  if (r == duckdberror) then
    call duckdb_disconnect(con)
    call duckdb_close(db)
    error stop "read error"
  end if

  nc = duckdb_result_chunk_count(res)
  do i = 0, nc - 1
    chk = duckdb_result_get_chunk(res, i)
    sc = duckdb_data_chunk_get_size(chk)
    cc = duckdb_data_chunk_get_column_count(chk)
    vec = duckdb_data_chunk_get_vector(chk, 3_int64)
    va = duckdb_vector_get_data(vec)
    vb = duckdb_vector_get_validity(vec)
    call c_f_pointer(va, vr, [sc])
    do j = 0, sc - 1
      if (duckdb_validity_row_is_valid(vb, j)) then
        print *, vr(j+1)
      else
        print *, "NULL"
      end if
    end do
    print *, i, j
    call duckdb_destroy_data_chunk(chk)
  end do

  call duckdb_destroy_result(res)
  call duckdb_disconnect(con)
  call duckdb_close(db)

  contains

    function read_csv(con, path, res) result(r)
      character(len=*) :: path
      character(len=:), allocatable :: sql
      type(duckdb_connection) :: con
      type(duckdb_result) :: res
      integer(kind(duckdb_state)) :: r
      sql = "select * from '" // path // "';" // c_null_char
      r = duckdb_query(con, sql, res)
      deallocate(sql)
    end function read_csv

end program main
