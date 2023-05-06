# duckdb
fortran bindings to duckdb c api

*still under development - contributions welcome*

### Implementation status

- [x] **test_starting_database.cpp**
  - [x] Simple In-Memory DB Start Up and Shutdown
  - [x] Multiple In-Memory DB Start Up and Shutdown
- [ ] **test_capi.cpp**
  - [x] Basic test of C API
  - [x] Test scalar NULL
  - [x] Test scalar string
  - [x] Test boolean
  - [x] Test multiple insert
  - [x] Test various error conditions
  - [x] Test integer columns 
  - [x] Test real/double columns 
  - [x] Test date columns 
  - [ ] Test time columns
  - [x] Test blob columns
  - [ ] Test boolean columns
  - [ ] Test decimal columns 
  - [ ] Test different types of C API
  - [ ] Test errors in C API
  - [ ] Test C API config
  - [ ] Issue #2058: Cleanup after execution of invalid SQL statement causes segmentation fault
  - [ ] Decimal -> Double casting issue
- [ ] **test_capi_data_chunk.cpp**
  - [x] Test table_info incorrect 'is_valid' value for 'dflt_value' column
  - [x] Test Logical Types C API
  - [ ] Test DataChunk C API (_in progress_)
  - [ ] Test DataChunk result fetch in C API
  - [ ] Test DataChunk populate ListVector in C API
- [ ] **capi_table_functions.cpp**
  - [ ] Test Table Functions C API
  - [ ] Test Table Function errors in C API
  - [ ] Test Table Function named parameters in C API
- [ ] **test_capi_appender.cpp**
  - [ ] Test appending into DECIMAL in C API
  - [ ] Test appender statements in C API
  - [ ] Test append timestamp in C API
- [ ] **test_capi_arrow.cpp**
  - [ ] Test arrow in C API
- [ ] **test_capi_complex_types.cpp**
  - [ ] Test decimal types C API
  - [ ] Test enum types C API
  - [ ] Test list types C API
  - [ ] Test struct types C API
- [ ] **test_capi_extract.cpp**
  - [ ] Test extract statements in C API
- [ ] **test_capi_pending.cpp**
  - [ ] Test pending statements in C API
- [ ] **test_capi_prepared.cpp**
  - [ ] Test prepared statements in C API
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

Requires the c library that can be downloaded from https://github.com/duckdb/duckdb/releases

Test with

```shell
fpm test
```
