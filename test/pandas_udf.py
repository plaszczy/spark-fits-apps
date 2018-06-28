
from pyspark.sql import SQLContext
from pyspark.sql.functions import col, count, rand, collect_list, explode, struct, count, lit
from pyspark.sql.functions import udf,pandas_udf, PandasUDFType

sqlContext = SQLContext.getOrCreate(sc)

#start
df = sqlContext.range(0, 1000)

@udf('int')
def plus_one(v):
      return v + 1

df.withColumn('udf', plus_one(df.id)).show()

@pandas_udf('int',PandasUDFType.SCALAR)
def pandas_plus_one(v):
    return v + 1

df.withColumn('pandas_udf', pandas_plus_one(df.id)).show()

import pandas as pd
from scipy import stats

@pandas_udf('double', PandasUDFType.SCALAR)
def pandas_cdf(v):
    return pd.Series(stats.norm.cdf(v))

df.withColumn('cumulative_probability', pandas_cdf(df.id)).agg(count(col('cumulative_probability'))).show()



#help

slen = pandas_udf(lambda s: s.str.len(), IntegerType())  # doctest: +SKIP

@pandas_udf(StringType())  # doctest: +SKIP
def to_upper(s):
      return s.str.upper()

@pandas_udf("float", PandasUDFType.SCALAR)  # doctest: +SKIP
def add_one(x):
      return x+1

import pandas as pd
@pandas_udf("float", PandasUDFType.SCALAR)  # doctest: +SKIP
def toBIBI(x):
      return pd.Series((x/2.))

df = spark.createDataFrame([(1, "John Doe", 21)],
                           ("id", "name", "age"))  # doctest: +SKIP
df.select(slen("name").alias("slen(name)"), to_upper("name"), add_one("age")).show()

