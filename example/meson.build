project('pary', 'fortran')
# subdir('src')
stdlib = dependency('fortran_stdlib')
duckdb = dependency('duckdb')
dep = [stdlib, duckdb]
arg = '-ffree-form'
exe = executable('pary',
  sources: files('duckdb.f', 'utils.f', 'main.f'),
  dependencies: dep, fortran_args: arg)
# exe = executable('ddb', sources: src, dependencies: dep)
