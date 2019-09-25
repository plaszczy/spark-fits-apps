from pyspark.sql.types import IntegerType
from pyspark.sql import functions as F

import numpy as np
import matplotlib
matplotlib.rcParams['image.interpolation']='nearest'
matplotlib.rcParams['image.cmap'] = 'jet' 
import matplotlib.pyplot as plt

def df_minmax(df,col):
    return df.select(F.min(col),F.max(col)).first()

def num_nans(df,col=None):
    if col==None:
        return df.count()-df.na.drop().count()
    else:
        #return df.filter(df[col].contains(np.nan)).count()
        return df.count()-df.select(col).na.drop().count()

def frac_nans(df,col=None):
    N=df.count()
    if col==None:
        return 1.-df.na.drop().count()/N
    else:
        #return df.filter(df[col].contains(np.nan)).count()
        return 1.-df.select(col).na.drop().count()/N

def df_hist(df,col,Nbins=50,bounds=None):

#drop nans if any
    df=df.select(col).na.drop()

    if (bounds==None) :
        m=df_minmax(df,col)
        print("{} : min/max=[{},{}]".format(col,m[0],m[1]))
        zmin=m[0]
        zmax=m[1]
    else:
        zmin=bounds[0] 
        zmax=bounds[1]
        df=df.filter(df[col].between(zmin,zmax))
    
    dz=(zmax-zmin)/Nbins
    X0=zmin+dz/2

#    zbin=df.select(df[col],(F.bround((df[col]-F.lit(X0))/dz),0).cast(IntegerType()).alias('bin'))
    zbin=df.withColumn("rbin",(df[col]-F.lit(X0))/dz)\
      .select(F.bround("rbin",0).cast(IntegerType()).alias('bin'))

    h=zbin.groupBy("bin").count().orderBy(F.asc("bin"))
    return h.select("bin",(F.lit(X0)+h['bin']*dz).alias('loc'),"count").drop("bin").toPandas(),dz


def df_histplot(df,col,Nbins=50,bounds=None,doStat=False):    
    result=df_hist(df,col,Nbins,bounds)
    hp=result[0]
    step=result[1]
    plt.figure()
    plt.bar(hp['loc'].values,hp['count'].values,step,color='white',edgecolor='black')
    plt.xlabel(col)
    if doStat:
        s=df.describe([col])
        s.show()
        r=s.select(col).take(6)
        N=int(r[0][0])
        mu=float(r[1][0])
        sig=float(r[2][0])
        min_=float(r[3][0])
        max_=float(r[4][0])
        stat=[r"$N={:g}$".format(N),r"$\mu={:g}$".format(mu),r"$\sigma={:g}$".format(sig),"min={:g}".format(min_),"max={:g}".format(max_)]
        ax=plt.gca()
        plt.text(0.8,0.7,"\n".join(stat), horizontalalignment='center',transform=ax.transAxes)
    plt.show()
    return hp

def df_histplot2(df,col1,col2,Nbin1=50,Nbin2=50,bounds=None,newfig=True,**kwargs):
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
    zbin=df.withColumn("rbin1",(df[col1]-zmin1-dz1/2)/dz1)\
      .select(F.bround("rbin1",0).cast(IntegerType()).alias('bin1'))\
      .withColumn("rbin2",(df[col2]-zmin2-dz2/2)/dz2)\
      .select(F.bround("rbin2",0).cast(IntegerType()).alias('bin2'))

    #count by bins
    h=zbin.groupBy("bin1","bin2").count()

    #plot
    hp=h.toPandas()
    m=np.zeros((Nbin1,Nbin2),dtype=int) 
    m[hp['bin1'].values,hp['bin2'].values]=hp['count'].values
    xx=zmin1+dz1/2+np.arange(Nbin1)*dz1
    yy=zmin2+dz2/2+np.arange(Nbin2)*dz2
    if newfig:
        plt.figure()
    plt.pcolormesh(xx,yy,m.T,**kwargs )
    plt.colorbar()
    plt.xlabel(col1)
    plt.ylabel(col2)
    plt.show()
    return xx,yy,m.T
