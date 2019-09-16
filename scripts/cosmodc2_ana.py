
#maps
#m=df.select(F.mean("ra"),F.mean("dec")).first()
#mean patch
rot=[61.81579482165925,-35.20157446022967]


#hitso mags
#for (b,c) in zip(bands,colbands):

for b in ['y']:
    plt.figure()
    h,step=df_hist(df,"mag_{}".format(b),bounds=(15,40))
    h_uf,step=df_hist(df.filter(df.halo_id<0),"mag_{}".format(b),bounds=(15,40))
    h_gal,step=df_hist(df.filter(df.halo_id>0),"mag_{}".format(b),bounds=(15,40))
    plt.bar(h['loc'],h['count'],step,label="total",color='b')
    plt.bar(h_uf['loc'],h_uf['count'],step,label="ultra faint",color='g')
    plt.bar(h_gal['loc'],h_gal['count'],step,label="gal",color='r',alpha=0.5)
    plt.xlabel("mag_{}".format(b))
    plt.legend()

#magnification
df_lens=df.withColumn("mag_lens-mag_true",df.mag_u-df.mag_true_u)
df_histplot(df_lens,"mag_lens-mag_true",bounds=(-0.4,0.5),Nbins=80,doStat=True)


#dfp=df.filter(df.ipix==38188148)
#dfp=dfp.withColumn('shift',dfp.mag_u-dfp.mag_true_u)
#p=dfp.toPandas()
#p_h=p[p['shift']<-1]

#strong lensing

#find hotspots
pix= df.filter(df.magnification>3).groupby("ipix").count().toPandas()
radec=hp.pix2ang(4096,pix.ipix.values,lonlat=True)
np.sort(radec[0])

hp.gnomview(m,rot=[50.14160156,-31.15903841],xsize=100,cmap='hot')

#colormag



#cosmo

h=df_histplot2(df,"log10z","m-M",Nbin1=500,Nbin2=500)

lz=h[0]
mm=h[1]

step=(mm-np.roll(mm,1))[-1]
mmin=mm[0]

omegaM=0.3
omegaL=0.7
H0=70.
c=300000.

q0=omegaM/2-omegaL


val=h[2]
for i in range(0,500):
    l=lz[i]
    s=25-5*np.log10(H0)+5*np.log10(c)+5*l+1.086*(1.-q0)*10**l
    j=int((s-mmin-step/2)/step)
    print(i,j,l,s)
    val[j,i]=1e6



#spectre
# z=1 et cut m?
df_map=df.filter(df.redshift.between(0.95,1.05)).select("ipix").groupBy("ipix").count()
p=df_map.toPandas()
m=np.mean(p['count'])

skyMap= np.full(hp.nside2npix(nside),hp.UNSEEN)

skyMap[p['ipix'].values]=p['count'].values/m-1.



N=200
hp.gnomview(skyMap,rot=rot,reso=reso,xsize=N)

c=hp.gnomview(skyMap,rot=rot,reso=reso,xsize=N,return_projected_map=True)


Ldeg=13.5
L=deg2rad(Ldeg)
area=L*L

k0=2*pi/L

kmax=k0*N/2*sqrt(2)

img=c.data

from scipy import fftpack
#ou np.fft?

#window
nx=np.hanning(N)
ny=np.hanning(N)
x,y=np.meshgrid(nx,ny)
hann=x*y
fapo=fftpack.fft2(hann)
w2=np.sum(abs(fapo)**2)

F1 = fftpack.fft2(img*hann)
F2 = fftpack.fftshift( F1 )
psd=np.abs( F2 )**2

freq=fftpack.fftshift(fftpack.fftfreq(N))*k0*N
plt.pcolormesh(freq,freq,psd/w2/area)
