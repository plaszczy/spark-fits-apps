from pyspark.sql.types import IntegerType
from pyspark.sql import functions as F

def minmax(df,col):
    return df.select(F.min(col),F.max(col)).first()

def df_histo(df,col,Nbins,bounds=None):

    if (bounds==None) :
        m=minmax(df,col)
        zmin=minmax[0]
        zmax=minmax[1]
    else:
        zmin=bounds[0]
        zmax=bounds[1]
        df=df.filter(df[col].between(zmin,zmax))
    
    dz=(zmax-zmin)/Nbins
    zbin=df.select(df[col],((df[col]-zmin-dz/2)/dz).cast(IntegerType()).alias('bin'))
    h=zbin.groupBy("bin").count().orderBy(F.asc("bin"))
    return h.select("bin",(zmin+dz/2+h['bin']*dz).alias('loc'),"count").drop("bin")
