from df_tools import * 
from tools import *
from histfile import *

crossstars="/lsst/DC2/run22xstars.parquet"

#cosmoXobj
df1=spark.read.parquet(crossstars)

#verif spatiale
df1=df1.withColumn("dx",F.degrees(F.sin((df1["theta_s"]+df1["theta_t"])/2)*(df1["phi_s"]-df1["phi_t"]))*3600)
df1=df1.withColumn("dy",F.degrees(df1["theta_s"]-df1["theta_t"])*3600)
x,y,m=df_histplot2(df1,"dx","dy",Nbin1=100,Nbin2=100,bounds=((-1,1),(-1,1)))
clf()
imshowXY(x,y,log10(1+m))

#cuts
df1=df1.filter(df1.r<0.1)
df1=df1.filter(df1.extendedness<0.1)
df1=df1.withColumnRenamed("i_smeared","mag_i_star").withColumnRenamed("r_smeared","mag_r_star")


#delta mags
df1=df1.withColumn("dmag_i",df1["mag_i_star"]-df1["mag_i"])
df1=df1.withColumn("dmag_r",df1["mag_r_star"]-df1["mag_r"])

df1=df1.withColumn("pull_mag_i",df1['dmag_i']/df1['magerr_i'])
df1=df1.withColumn("pull_mag_r",df1['dmag_r']/df1['magerr_r'])

df1=df1.withColumn("flux_i_star",F.pow(10.0,-(df1["mag_i_star"]-31.4)/2.5))
df1=df1.withColumn("pull_flux_i",(df1['flux_i_star']-df1['psFlux_i'])/df1['psFluxErr_i'])

df1.cache().count()

#delt mag
#2d
x,y,m=df_histplot2(df1,"dmag_i","mag_i_star",bounds=((-0.1,0.1),(15,23)),Nbin1=200,Nbin2=200)
clf()
imshowXY(x,y,log10(1+m))
xlabel("mag_i_star-mag_i")
title(crossstars)

#1d
p=df_histplot(df1,"dmag_i",Nbins=500,bounds=[-0.05,0.05])
title(crossstars)
xlabel("mag_i_star-mag_i")
s=addStat(p['loc'],p['count'])
axvline(-0.002,color='r')


#pull
p=df_histplot(df1.filter(df1.mag_i_star>17),"pull_mag_i",Nbins=100,bounds=[-10,10])
s=addStat(p['loc'],p['count'])

p=df_histplot(df1.filter(df1.mag_i_star>17),"pull_flux_i",Nbins=100,bounds=[-10,10])
s=addStat(p['loc'],p['count'])
x=p['loc'].values
y=p['count'].values
ymax=max(y)
plot(x,ymax*exp(-x**2/2))
#xlabel("(flux(rec)-flux(true))/sigma(flux)")


#astro vs photo
x,y,m=df_histplot2(df1.filter(df1.snr_i_cModel>10),"dmag_i","r",bounds=((-0.2,0.2),(0,0.1)),Nbin1=200,Nbin2=200)
title(r"SNR>10")
x,y,m=df_histplot2(df1,"dmag_i","snr_i_cModel",bounds=((-0.2,0.2),(0,100)),Nbin1=200,Nbin2=200)




#colors

x,y,m=df_histplot2(df1,"d(r-i)","r",bounds=((-0.2,0.2),(0,0.1)),Nbin1=200,Nbin2=200)

x,y,m=df_histplot2(df1,"d(r-i)","snr_i_cModel",bounds=((-0.5,0.5),(5,30)),Nbin1=200,Nbin2=200)
