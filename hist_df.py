from pyspark.sql.types import IntegerType
from pyspark.sql import functions as F

def hist_df(df,col,Nbins,zmin=None,zmax=None):
    if (zmin==None or zmax==None) :
        minmax=df.select(F.min(col),F.max(col)).first()
        if (zmin==None) :
            zmin=minmax[0]
        if (zmax==None) :
            zmax=minmax[1] 
    dz=(zmax-zmin)/Nbins
    zbin=df.select(df[col],((df[col]-zmin)/dz).cast(IntegerType()).alias('bin'))
    h=zbin.groupBy("bin").count().orderBy(F.asc("bin"))
    return h.select("bin",(zmin+dz/2+h['bin']*dz).alias('zbin'),"count")\
           .drop("bin")
