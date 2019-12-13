from df_tools import * 
from tools import *
from histfile import *

df1=spark.read.parquet("/lsst/DC2/df4.parquet")


df1=spark.read.parquet("/lsst/DC2/newstars.parquet")
df1=df1.withColumnRenamed("i_smeared","mag_i")
df1=df1.withColumnRenamed("r_smeared","mag_r")


#cuts
df1=df1.filter((df1["snr_i_cModel"]>1) & (df1["snr_r_cModel"]>1))

df1=df1.filter( (df1["clean"]==1)) 
#df1=df1.filter(df1["extendedness"]==1)

df1=df1.withColumn("flux_i",F.pow(10.0,-(df1["mag_i"]-31.4)/2.5))

#cmodel
#df1=df1.withColumn("dflux",df1["cModelFlux_i"]-df1["flux_i"])
#df1=df1.withColumn("dmag_i",df1["mag_i_cModel"]-df1["mag_i"])
#df1=df1.withColumn("dmag_r",df1["mag_r_cModel"]-df1["mag_r"])

#psf quantities
df1=df1.withColumn("dflux",df1["psFlux_i"]-df1["flux_i"])
df1=df1.withColumn("mag_i_psf",-2.5*F.log10(df1.psFlux_i)+31.4)
df1=df1.withColumn("mag_r_psf",-2.5*F.log10(df1.psFlux_r)+31.4)
df1=df1.withColumn("dmag_i",df1["mag_i_psf"]-df1["mag_i"])
df1=df1.withColumn("dmag_r",df1["mag_r_psf"]-df1["mag_r"])



#df1=df1.withColumn("sigpos",df1["sigr"]/df1["snr_i_cModel"]).drop("sigr")
df1=df1.withColumn("dx",F.degrees(F.sin((df1["theta_s"]+df1["theta_t"])/2)*(df1["phi_s"]-df1["phi_t"]))*3600)
df1=df1.withColumn("dy",F.degrees(df1["theta_s"]-df1["theta_t"])*3600)
df1=df1.withColumn("psf_x",df1["dx"]*df1["snr_i_cModel"]/sqrt(2.))
df1=df1.withColumn("psf_y",df1["dy"]*df1["snr_i_cModel"]/sqrt(2.))

df1=df1.withColumn("r-i_true",df1['mag_r']-df1['mag_i'])
df1=df1.withColumn("r-i_rec",df1['mag_r_cModel']-df1['mag_i_cModel'])
df1=df1.withColumn("d(r-i)",df1['r-i_rec']-df1['r-i_true'])

#df1=df1.filter(df1.r<0.6)

df1.cache().count()

# pixel borders
x,y,m=df_histplot2(df1,"dx","dy",Nbin1=100,Nbin2=100,bounds=((-2.5,2.5),(-2.5,2.5))) 
clf()
imshowXY(x,y,log10(1+m))
#zoom avec cut r<1
x,y,m=df_histplot2(df1,"dx","dy",Nbin1=100,Nbin2=100,bounds=((-1,1),(-1,1)))
clf()
imshowXY(x,y,log10(1+m))


#r-flux
x,y,m=df_histplot2(df1,"dflux","r",Nbin1=100,Nbin2=100,bounds=((-500,500),(0,1.)))

imshowXY(x,y,log10(m+1))    

#w/o flux cuts
df1=df1.filter(df1.r<1)
p1=df_histplot(df1,"dx",Nbins=1001,bounds=(-1,1))
p2=df_histplot(df1.filter((df1["dflux"]<500)&(df1["dflux"]>-500)),"dx",Nbins=1001,bounds=(-1,1))
p3=df_histplot(df1.filter((df1["dflux"]<200)&(df1["dflux"]>-250)),"dx",Nbins=1001,bounds=(-1,1))


hist_stat(p1['loc'].values,1001*p1['count'].values/sum(p1['count'].values),log=False) 
hist_stat(p2['loc'].values,1001*p2['count'].values/sum(p2['count'].values),log=False,newFig=False,doStat=False)
hist_stat(p3['loc'].values,1001*p3['count'].values/sum(p3['count'].values),log=False,newFig=False,doStat=False)
ylim(0,0.2)
xlabel("dx [arcsec]")

#psf
df1=df1.filter(df1.r<0.6)


x,y,m=df_histplot2(df1,"psf_x","psf_y",Nbin1=100,Nbin2=100,bounds=((-1,1),(-1,1))

figure()
X,Y=meshgrid(x,y)
R=sqrt(X**2+Y**2)
plot(R.flat,m.flat/amax(m),'k+')
xlim(0)
ylim(0)
axhline(0.5,ls='--',c='k')

r=linspace(0,2.8,100)
fwhm=0.47*2

sig=fwhm/2.355
plot(r,exp(-r**2/(2*sig**2)),label="Gaussian")


for b in [4.765,2] :
    a=fwhm/(2*sqrt(2**(1/b)-1))
    plot(r,1./(1+(r/a)**2)**b,label=r"Moffat $(\beta={})$)".format(b))


#histo dr?
df1=df1.withColumn("psf_r",df1["r"]*df1["snr_i_cModel"]/sqrt(2.))
p=df_histplot(df1,"psf_r",Nbins=101,bounds=(0,5))


#pull
df1=df1.withColumn("pull_r",df1["psf_r"]/df1["psf_fwhm_i"])
p=df_histplot(df1,"pull_r",Nbins=101,bounds=(0,5))

p=df_histplot(df1.withColumn("pullx",df1["dx"]/df1["sigpos"]),"pullx",bounds=(-10,10),Nbins=1001)
addStat(p['loc'].values,p['count'])


#mag vs snr

df1=df1.withColumn("pull_flux_i",df1['dflux']/df1['cModelFluxErr_i'])
df1=df1.withColumn("pull_mag_i",df1['dmag_i']/df1['magerr_i_cModel'])

snrcut=arange(10,31,4,dtype=float64)

figure()
for (v1,v2) in zip(snrcut[:-1],roll(snrcut,-1)):
    print(v1,v2)
#    p=df_hist(df1.filter(df1["snr_i_cModel"].between(v1,v2)),"dmag",bounds=(-1.5,1),Nbins=300)
#    p=df_hist(df1.filter(df1["snr_i_cModel"].between(v1,v2)),"dflux",bounds=(-1000,1000),Nbins=300)
    p=df_hist(df1.filter(df1["snr_i_cModel"].between(v1,v2)),"pull_flux",bounds=(-10,10),Nbins=300)
    x=p[0]['loc'].values
    y=p[0]['count'].values
    bar_outline(x,y/sum(y),label=r"{}<SNR<{}".format(int(v1),int(v2)))
axvline(0,c='k',ls='--')
y=exp(-x**2/2)
#plot(x,y/sum(y),'k--')
plot(x,y*.011,'k--')

legend()

xlabel("(mag(rec)-mag(true))/sigmag")
xlabel("(flux(rec)-flux(true))/sigma(flux)")
#astro vs photo
x,y,m=df_histplot2(df1.filter(df1.snr_i_cModel>10),"dmag_i","r",bounds=((-0.2,0.2),(0,0.1)),Nbin1=200,Nbin2=200)
title(r"SNR>10")
x,y,m=df_histplot2(df1,"dmag_i","snr_i_cModel",bounds=((-0.2,0.2),(0,100)),Nbin1=200,Nbin2=200)




#colors

x,y,m=df_histplot2(df1,"d(r-i)","r",bounds=((-0.2,0.2),(0,0.1)),Nbin1=200,Nbin2=200)

x,y,m=df_histplot2(df1,"d(r-i)","snr_i_cModel",bounds=((-0.5,0.5),(5,30)),Nbin1=200,Nbin2=200)
