from pyspark.sql.types import IntegerType
from pyspark.sql import functions as F

def minmax(df,col):
    return df.select(F.min(col),F.max(col)).first()

def hist_df(df,col,Nbins,zmin=None,zmax=None):
    if (zmin==None or zmax==None) :
        m=minmax(df,col)
        if (zmin==None) :
            zmin=m[0]
        if (zmax==None) :
            zmax=m[1] 
    dz=(zmax-zmin)/Nbins
    zbin=df.select(df[col],((df[col]-zmin)/dz).cast(IntegerType()).alias('bin'))
    h=zbin.groupBy("bin").count().orderBy(F.asc("bin"))
    return h.select("bin",(zmin+dz/2+h['bin']*dz).alias('zbin'),"count").drop("bin")
