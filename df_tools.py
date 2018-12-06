from pyspark.sql.types import IntegerType
from pyspark.sql import functions as F

import matplotlib.pyplot as plt

def minmax(df,col):
    return df.select(F.min(col),F.max(col)).first()

def df_histo(df,col,Nbins,bounds=None):

    if (bounds==None) :
        m=minmax(df,col)
        zmin=m[0]
        zmax=m[1]
    else:
        zmin=bounds[0]
        zmax=bounds[1]
        df=df.filter(df[col].between(zmin,zmax))
    
    dz=(zmax-zmin)/Nbins
    zbin=df.select(df[col],((df[col]-zmin-dz/2)/dz).cast(IntegerType()).alias('bin'))
    h=zbin.groupBy("bin").count().orderBy(F.asc("bin"))
    return h.select("bin",(zmin+dz/2+h['bin']*dz).alias('loc'),"count").drop("bin")


def plot_histo(df,col,Nbins,bounds=None):

    if (bounds==None) :
        m=minmax(df,col)
        zmin=m[0]
        zmax=m[1]
        print("min/max bounds=[{},{}]".format(zmin,zmax))
    else:
        zmin=bounds[0]
        zmax=bounds[1]
        df=df.filter(df[col].between(zmin,zmax))
    
    step=(zmax-zmin)/Nbins
    zbin=df.select(df[col],((df[col]-zmin-step/2)/step).cast(IntegerType()).alias('bin'))
    h=zbin.groupBy("bin").count().orderBy(F.asc("bin"))
    hp=h.select("bin",(zmin+step/2+h['bin']*step).alias('loc'),"count").drop("bin").toPandas()
    plt.bar(hp['loc'].values,hp['count'].values,step,color='white',edgecolor='black')
    plt.xlabel(col)
    plt.show()
    return hp
