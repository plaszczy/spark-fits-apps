from numpy import *
from numpy.fft import fft2,fftshift,fftfreq
import pandas as pd
import tools
import healpy as hp

#cosmodc2
#rot=[61.81579482165925,-35.20157446022967]
#nside=512
#pixarea=hp.nside2pixarea(nside, degrees=True)*3600
#reso= hp.nside2resol(nside,arcmin=True)
#print("nside={}".format(nside))

zmean=0.5
dz=0.1

zcut=[zmean-dz,zmean+dz]
angpow="dc2_z{}_smooth.fits".format(zmean)

df_high=df.filter((F.log10("stellar_mass")>10.5) & (df.is_central==True))
df_map=df_high.filter(df.redshift.between(zcut[0],zcut[1])).select("ipix").groupBy("ipix").count()



p=df_map.toPandas()
#sans rescale
skyMap= full(hp.nside2npix(nside),hp.UNSEEN)
skyMap[p['ipix'].values]=p['count'].values


#gnome proj (rot,reso,pixarea)
Npix=150

#Ldeg=13.5
Ldeg=sqrt(pixarea)*Npix/60
print("L={} deg".format(Ldeg))
L=deg2rad(Ldeg)
L2=L*L

k0=2*pi/L
kmax=k0*Npix/2
print("k0={} kmax={}".format(k0,kmax))

c=hp.gnomview(skyMap,rot=rot,reso=reso,xsize=Npix,return_projected_map=True,fig=1)


assert c.mask.sum()==0
Nmean=c.data.mean()
Ntot=c.data.sum()
#rescale
img=c.data/Nmean-1.

dens=Ntot/Ldeg**2/3600
print("mean density on patch={} gals/arcmin**2".format(dens))

#in correct units
Nbar=Ntot/L2


#2D
#window
nx=kaiser(Npix,10)

x,y=meshgrid(nx,nx)
hann=x*y
fapo=fft2(hann)
w2=sum(abs(fapo)**2)

#fft
F1 = fft2(img*hann)
F2 = fftshift( F1 )
psd=abs(F2)**2/w2*L2

freq=fftshift(fftfreq(Npix)*Npix*k0)

## plt.figure()
## plt.pcolormesh(freq,freq,psd,vmax=1e-6)
## plt.colorbar()
## plt.title("{}<z<{}".format(zcut[0],zcut[1]))
## plt.xlabel(r"$\ell_x$")
## plt.ylabel(r"$\ell_y$")
## plt.tight_layout()


#1D##########
#
x,y=meshgrid(freq,freq)
kmap=sqrt(x**2+y**2)

#binning in k
kbin=arange(0,kmax,50)


kmean=[]
psmean=[]
stdps=[]
for k1,k2 in zip(kbin[0:-1],roll(kbin,-1)):
    w=where(logical_and(kmap>k1,kmap<k2))
    kmean.append(mean(kmap[w].flat))
    psmean.append(mean(psd[w].flat))
    stdps.append(std(psd[w].flat))
#
kmean=array(kmean)
psmean=array(psmean)
stdps=array(stdps)


plt.figure()
t=tools.mrdfits(angpow,1,silent=True)
plt.plot(t.ell,t.cl00,'r',label='AngPow')
plt.errorbar(kmean,psmean-1/Nbar,yerr=stdps,xerr=25,fmt='o',c='k',label='cosmoDC2')
plt.xlabel(r"$\ell$")
plt.ylabel(r"$C_\ell$")
plt.title("{}<z<{}".format(zcut[0],zcut[1]))
#plt.axhline(1/Nbar,c='k',lw=0.5)
plt.xlim(0,kmax)
plt.legend()
plt.tight_layout()

#biais
plt.figure()
ck=interp(kmean,t.ell,t.cl00)
#plt.plot(kmean,(psmean-1/Nbar)/ck,label="{}<z<{}".format(zcut[0],zcut[1]))
plt.errorbar(kmean,(psmean-1/Nbar)/ck,yerr=stdps,xerr=25,fmt='o',c='k',label='bias')
