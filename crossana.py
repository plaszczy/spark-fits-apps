from df_tools import * 
from tools import *


df1=spark.read.parquet("/lsst/DC2/df1.parquet")

df1=df1.filter(df1["snr_i_cModel"]>1) 

df1=df1.filter(df1["mag_i_cModel"]<24.1)
df1=df1.filter( (df1["clean"]==1) & (df1["extendedness"]==1)) 


df1=df1.withColumn("flux_i",F.pow(10.0,-(df1["mag_i"]-31.4)/2.5))
df1=df1.withColumn("dflux",df1["cModelFlux_i"]-df1["flux_i"])
df1=df1.withColumn("dmag",df1["mag_i_cModel"]-df1["mag_i"])
df1=df1.withColumn("sigpos",df1["sigr"]/df1["snr_i_cModel"]).drop("sigr")
df1=df1.withColumn("dphi",F.degrees(F.sin((df1["theta_s"]+df1["theta_t"])/2)*(df1["phi_s"]-df1["phi_t"]))*3600)
df1=df1.withColumn("dtet",F.degrees(df1["theta_s"]-df1["theta_t"])*3600)
df1.cache().count()

x,y,m=df_histplot2(df1,"dmag","r",Nbin1=100,Nbin2=100,bounds=((-2,2),(0,5))) 
clf()
imshowXY(x,y,log10(m+1))    

#extra cuts
df1=df1.filter(df1["r"]<1) 
df1=df1.filter((F.abs(df1["dmag"]<1))

df_histplot,"dtet",Nbins=1000,bounds=(-5,5))


x,y,m=df_histplot2(df1,"dtet","dphi",Nbin1=100,Nbin2=100,bounds=((-1,1),(-1,1))) 
clf()
imshowXY(x,y,log10(1+m))


#flux
x,y,m=df_histplot2(df1,"dflux","r",Nbin1=100,Nbin2=100,bounds=((-3000,3000),(0,1))) 
clf()
imshowXY(x,y,log10(1+m))
xlabel(r"$\Delta DEC$")
ylabel(r"$\cos(DEC).\Delta RA$")

#outliers
