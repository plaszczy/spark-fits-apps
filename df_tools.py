from pyspark.sql.types import IntegerType
from pyspark.sql import functions as F

import numpy as np
import matplotlib
matplotlib.rcParams['image.interpolation']='nearest'
matplotlib.rcParams['image.cmap'] = 'jet' 
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
    return h.select("bin",(zmin+dz/2+h['bin']*dz).alias('loc'),"count").drop("bin"),dz,(zmin,zmax)


def plot_histo(df,col,Nbins=50,bounds=None):    
    result=df_histo(df,col,Nbins,bounds)
    hp=result[0].toPandas()
    step=result[1]
    plt.bar(hp['loc'].values,hp['count'].values,step,color='white',edgecolor='black')
    plt.xlabel(col)
    plt.show()
    return hp

def plot_histo2(df,col1,col2,Nbin1=50,Nbin2=50,bounds=None):
    if (bounds==None) :
        b=df.select(F.min(col1),F.max(col1),F.min(col2),F.max(col2)).first()
        zmin1=b[0]
        zmax1=b[1]
        zmin2=b[2]
        zmax2=b[3]
    else:
        zmin1=bounds[0][0]
        zmax1=bounds[0][1]
        zmin2=bounds[1][0]
        zmax2=bounds[1][1]
        df=df.filter((df[col1].between(zmin1,zmax1)) & (df[col2].between(zmin2,zmax2)))
    
    dz1=(zmax1-zmin1)/Nbin1
    dz2=(zmax2-zmin2)/Nbin2
    
    #add bins columns    
    zbin=df.select( ((df[col1]-zmin1-dz1/2)/dz1).cast(IntegerType()).alias('bin1'),\
                    ((df[col2]-zmin2-dz2/2)/dz2).cast(IntegerType()).alias('bin2') )
    #count by bins
    h=zbin.groupBy("bin1","bin2").count()

    #plot
    hp=h.toPandas()
    m=np.zeros((Nbin1,Nbin2),dtype=int) 
    m[hp['bin1'].values,hp['bin2'].values]=hp['count'].values
    xx=zmin1+dz1/2+np.arange(Nbin1)*dz1
    yy=zmin2+dz2/2+np.arange(Nbin2)*dz2
    plt.pcolormesh(xx,yy,m)
    plt.colorbar()
    plt.xlabel(col1)
    plt.ylabel(col2)
    plt.show()
