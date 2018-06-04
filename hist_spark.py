from pyspark.sql.types import IntegerType
from pyspark.sql import functions as F

def hist_spark(df,col,Nbins,zmin=None,zmax=None):
    if (zmin==None or zmax==None) :
        minmax=df.select(F.min(col),F.max(col)).first()
        if (zmin==None) :
            zmin=minmax[0]
        if (zmax==None) :
            zmax=minmax[1] 
    dz=(zmax-zmin)/Nbins
    zbin=df.select(df[col],((df[col]-zmin)/dz).cast(IntegerType()).alias('bin'))
    h=zbin.groupBy("bin").count().orderBy(F.asc("bin"))
    p=h.toPandas()
    # return to z values
    p['bin']=p['bin'].apply(lambda b:zmin+dz/2+b*dz)
    return p
