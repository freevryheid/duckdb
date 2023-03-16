# duckdb
fortran bindings to duckdb c api

still under development

you'll need the c library that can be downloaded from https://duckdb.org/docs/installation/


fpm build
fpm test
fpm run main

dev notes:
1. see FIXMEs
2. print* in main added to prevent memory *smashing* errors (not sure why this is)


