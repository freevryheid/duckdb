import pyarrow.parquet as pq; 

p1 = pq.read_table('test.parquet')
p2 = pq.read_table('result.parquet')

assert p1.equals(p2)
