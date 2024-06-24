# duckdb
fortran bindings to duckdb c api

## Introduction
[DuckDB](https://duckdb.org/), at the timne of this writing, is at
version 1.0.0 (stable release). The fortran module in this repository wraps the
[C-API](https://duckdb.org/docs/api/c/overview). While it is still under
develpment, most of the api functions have been wrapped, allowing access to
DuckDB databases, querying and data extraction.

DuckDB provides column-based data storage in contrast to other databases
(SQLIte, PostgreSQL, etc) that are row-based. To use the API requires some
basic understanding of the data structure.

### Data structure

DuckDB allows extracting data directly from csv, parquet and json files,
even if compressed:

```SQL
select * from 'data.csv.gz'
```

DuckDB databases can be :in-memory: or file-based. The wrapper provides
functions to initialize the database and connection as shown below. In-memory
databases are used if no path is provided in the [duckdb_open]
(https://github.com/freevryheid/duckdb/blob/5badd01d53001373d36dd50a731773a4be9ffa29/src/duckdb.f90#L2681)
function, which is optional.

```fortran

type(duckdb_database) :: db
type(duckdb_connection) :: con

if (duckdb_open(db=db) == duckdberror) then
  error stop "open error"
end if

if (duckdb_connect(db, con) == duckdberror) then
  call duckdb_close(db)
  error stop "connect error"
end if

call duckdb_disconnect(con)
call duckdb_close(db)

```

Once a connection to the database has been established, it can be queried
to extract results. Note that SQL strings passed to DuckDB must be
null-terminated.

```fortran
use, intrinsic :: iso_c_binding

type(duckdb_connection) :: con
type(duckdb_result) :: res
integer(kind(duckdb_state)) :: ri

sql = "select * from '" // path // "';" // c_null_char
r = duckdb_query(con, sql, res)

deallocate(sql)
```

The recommended way to interact with result sets is using chunks and vectors.
In DuckDB a chunk is defined as a dataset having a fixed number of rows. This
number is configurable but is set at 2048 by default. Chunks can be extracted
from result sets using the following functions:

```fortran
  use, intrinsic :: iso_fortran_env, only : int64
  type(duckdb_data_chunk) :: chk ! chunk
  integer(kind=int64) :: i, nc
  type(duckdb_result) :: res ! result set

  nc = duckdb_result_chunk_count(res) ! number of chunks
  do i = 0_int64, nc
    chk = duckdb_result_get_chunk(res, i)
    ! do somethink with chunk
    ! ...
    call duckdb_destroy_data_chunk(chk)
  end do
```

Vectors in turn are extracted from chunks. DuckDB vectors are column based data
having a specific data type. It is important to define the data type in fortran
when extracting these vectors. Consider the folllowing dataset that comprises
3 vectors, all of type int64.

┌─────────┬─────────┬─────────┐
│ column0 │ column1 │ column2 │
│  int64  │  int64  │  int64  │
├─────────┼─────────┼─────────┤
│       1 │       2 │       3 │
│       4 │       5 │       6 │
│       7 │       8 │       9 │
└─────────┴─────────┴─────────┘

The functions below outline one possible way to extract data from vectors.
These data are returned as a c_ptr, which could be converted into a fortran
pointer without the need for allocating additional memory. These pointers will
only be available though while the chunk is still active and are lost if the
chunk is destroyed or if a new chunk is extracted from the result set.

The code below demonstrates how vector data pointers, that may comprise
multiple columns, may be consumed by fortran using a derived type defined in
the data type of the result set. Note that DuckDB provides functions to check
the validity of the data which could include missing or NULL data.

```fortran

type vectors
  integer(kind=int64), pointer, dimension(:) :: ptr
end type

type(duckdb_data_chunk) :: chk
type(vectors), allocatable, dimension(:) :: vecs
type(duckdb_vector) :: vec
integer(kind=int64) :: j, rows, cols
type(c_ptr) :: va !, vb
integer(kind=int64), pointer, dimension(:) :: a
integer(kind=int64), pointer, dimension(:,:) :: mat ! 2d array
integer :: i
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

if (allocated(vecs)) then
  allocate(mat(3,3))
  do i = 1, 3
    mat(1:3, i) = vecs(i)%ptr
  end do

  print *, mat

  deallocate(mat)

end if

deallocate(vecs)

call duckdb_destroy_data_chunk(chk)
call duckdb_destroy_result(res)
call duckdb_disconnect(con)
call duckdb_close(db)
```

An example of extracting data from a csv file is provided in the [example]
(https://github.com/freevryheid/duckdb/tree/main/example) folder.

### Implementation status

- [x] **test_starting_database.cpp**
  - [x] Simple In-Memory DB Start Up and Shutdown
  - [x] Multiple In-Memory DB Start Up and Shutdown
- [x] **test_capi.cpp**
  - [x] Basic test of C API
  - [x] Test scalar NULL
  - [x] Test scalar string
  - [x] Test boolean
  - [x] Test multiple insert
  - [x] Test various error conditions
  - [x] Test integer columns
  - [x] Test real/double columns
  - [x] Test date columns
  - [x] Test time columns
  - [x] Test blob columns
  - [x] Test boolean columns
  - [x] Test decimal columns
  - [x] Test errors in C API
  - [x] Test C API config
  - [x] ~~Issue #2058: Cleanup after execution of invalid SQL statement causes segmentation fault~~
  - [x] ~~Decimal -> Double casting issue~~
- [ ] **test_capi_data_chunk.cpp**
  - [x] Test table_info incorrect 'is_valid' value for 'dflt_value' column
  - [x] Test Logical Types C API
  - [x] Test DataChunk C API
  - [ ] Test DataChunk varchar result fetch in C API
  - [x] Test DataChunk result fetch in C API
  - [x] Test DataChunk populate ListVector in C API
- [ ] **capi_table_functions.cpp**
  - [ ] Test Table Functions C API
  - [ ] Test Table Function errors in C API
  - [ ] Test Table Function named parameters in C API
- [x] **test_capi_appender.cpp**
  - [x] Test appending into DECIMAL in C APIg  - [x] Test appender statements in C API
  - [x] Test append timestamp in C API
- [ ] **test_capi_arrow.cpp**
  - [ ] Test arrow in C API
- [x] **test_capi_complex_types.cpp**
  - [x] Test decimal types C API
  - [x] Test enum types C API
  - [x] Test list types C API
  - [x] Test struct types C API
- [x] **test_capi_extract.cpp**
  - [x] Test extract statements in C API
- [x] **test_capi_pending.cpp**
  - [x] Test pending statements in C API
- [x] **test_capi_prepared.cpp**
  - [x] Test prepared statements in C API
- [ ] **test_capi_replacement_scan.cpp**
  - [ ] Test replacement scans in C API
  - [ ] Test error replacement scan
- [ ] **test_capi_streaming.cpp**
  - [ ] Test streaming results in C API
  - [ ] Test other methods on streaming results in C API
- [ ] **test_capi_to_decimal.cpp**
  - [ ] Test CAPI duckdb_decimal_as_properties
- [ ] **test_capi_website.cpp**
  - [ ] Test C API examples from the website


### Setup and test

Requires the c library that can be downloaded from https://github.com/duckdb/duckdb/releases. If you're on archlinux you can install the libraries and headers using "yay duckdb-bin", which includes the cli binary.

**Minimum Duckdb version required: 0.8**

Test with

```shell
fpm test
```

To include this in your own projects, add this dependency to your fpm.toml:

```shell
[dependencies]
duckdb.git = "https://github.com/freevryheid/duckdb"
```
