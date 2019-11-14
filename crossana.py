from df_tools import * 
from tools import *
from histfile import *

#df1=spark.read.parquet("/lsst/DC2/df1.parquet")
df1=spark.read.parquet("/lsst/DC2/df2.parquet")

df1=df1.filter(df1["snr_i_cModel"]>1) 

#df1=df1.filter(df1["mag_i_cModel"]<24.1)
df1=df1.filter( (df1["clean"]==1) & (df1["extendedness"]==1)) 


df1=df1.withColumn("flux_i",F.pow(10.0,-(df1["mag_i"]-31.4)/2.5))
df1=df1.withColumn("dflux",df1["cModelFlux_i"]-df1["flux_i"])
df1=df1.withColumn("dmag",df1["mag_i_cModel"]-df1["mag_i"])
df1=df1.withColumn("sigpos",df1["sigr"]/df1["snr_i_cModel"]).drop("sigr")
df1=df1.withColumn("dx",F.degrees(F.sin((df1["theta_s"]+df1["theta_t"])/2)*(df1["phi_s"]-df1["phi_t"]))*3600)
df1=df1.withColumn("dy",F.degrees(df1["theta_s"]-df1["theta_t"])*3600)
df1=df1.withColumn("psf_x",df1["dx"]*df1["snr_i_cModel"])
df1=df1.withColumn("psf_y",df1["dy"]*df1["snr_i_cModel"])

df1.cache().count()

# pixel borders
x,y,m=df_histplot2(df1,"dx","dy",Nbin1=100,Nbin2=100,bounds=((-2.5,2.5),(-2.5,2.5))) 
clf()
imshowXY(x,y,log10(1+m))
#zoom avec cut r<1
x,y,m=df_histplot2(df1.filter(df1.r<1),"dx","dy",Nbin1=100,Nbin2=100,bounds=((-1,1),(-1,1))
clf()
imshowXY(x,y,log10(1+m))


#r-flux
x,y,m=df_histplot2(df1,"dflux","r",Nbin1=100,Nbin2=100,bounds=((-500,500),(0,1.5)))

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

#flux distribs

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
fwhm=0.67*2

sig=fwhm/2.355
plot(r,exp(-r**2/(2*sig**2)),label="Gaussian")


for b in [4.765,2] :
    a=fwhm/(2*sqrt(2**(1/b)-1))
    plot(r,1./(1+(r/a)**2)**b,label=r"Moffat $(\beta={})$)".format(b))


#histo dr?
df1=df1.withColumn("psf_r",df1["r"]*df1["snr_i_cModel"])
p=df_histplot(df1,"psf_r",Nbins=1001,bounds=(0,5))


#pull
p=df_histplot(df1.withColumn("pullx",df1["dx"]/df1["sigpos"]),"pullx",bounds=(-10,10),Nbins=1001)
addStat(p['loc'].values,p['count'])
