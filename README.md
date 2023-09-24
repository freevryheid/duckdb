# duckdb
fortran bindings to duckdb c api

*still under development - contributions welcome*

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
- [ ] **test_capi_appender.cpp**
  - [ ] Test appending into DECIMAL in C API
  - [x] Test appender statements in C API
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
- [ ] **test_capi_pending.cpp**
  - [ ] Test pending statements in C API
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
