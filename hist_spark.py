from pyspark.sql.types import IntegerType

def hist_spark(df,col,Nbin):
    minmax=df.select(F.min(col),F.max(col)).first()
    zmin=minmax[0]
    zmax=minmax[1]
    dz=(zmax-zmin)/Nbins
    zbin=gal.select(df[col],((df[col]-zmin)/dz).cast(IntegerType()).alias('bin'))\
          .cache()
    h=zbin.groupBy("bin").count().orderBy(F.asc("bin"))
    return h.toPandas()
