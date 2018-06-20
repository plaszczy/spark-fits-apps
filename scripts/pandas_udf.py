
from pyspark.sql import SQLContext
from pyspark.sql.functions import col, count, rand, collect_list, explode, struct, count, lit
from pyspark.sql.functions import udf,pandas_udf, PandasUDFType

sqlContext = SQLContext.getOrCreate(sc)

#start
df = sqlContext.range(0, 1000)

@udf('int')
def plus_one(v):
      return v + 1

df.withColumn('v2', plus_one(df.id)).show()

@pandas_udf('int', PandasUDFType.SCALAR)
def pandas_plus_one(v):
    return v + 1

df.withColumn('v3', pandas_plus_one(df.id)).show()


import pandas as pd
from scipy import stats

@pandas_udf('double', PandasUDFType.SCALAR)
def pandas_cdf(v):
    return pd.Series(stats.norm.cdf(v))

df.withColumn('cumulative_probability', pandas_cdf(df.id)).agg(count(col('cumulative_probability'))).show()
