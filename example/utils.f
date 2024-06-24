module utils
  use, intrinsic :: iso_fortran_env
  use, intrinsic :: iso_c_binding
  use duckdb
  implicit none

  type vectors
    integer(kind=int64), pointer, dimension(:) :: ptr
  end type

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

    subroutine chk2vecs(chk, vecs)
      type(duckdb_data_chunk) :: chk
      type(vectors), allocatable, dimension(:) :: vecs
      type(duckdb_vector) :: vec
      integer(kind=int64) :: j, rows, cols
      type(c_ptr) :: va !, vb
      integer(kind=int64), pointer, dimension(:) :: a
      rows = duckdb_data_chunk_get_size(chk)
      cols = duckdb_data_chunk_get_column_count(chk)
      allocate(vecs(cols))
      do j = 0_int64, cols - 1
        vec = duckdb_data_chunk_get_vector(chk, j)
        va = duckdb_vector_get_data(vec)
        ! vb = duckdb_vector_get_validity(vec)
        call c_f_pointer(va, a, [rows])
        vecs(j+1)%ptr => a
        ! not cared with validity check
        ! do k = 0, sc - 1
        ! if (duckdb_validity_row_is_valid(vb, k)) then
        !   print *, vr(k+1)
        ! else
        !   print *, "NULL"
        ! end if
      end do
    end subroutine chk2vecs

end module utils

