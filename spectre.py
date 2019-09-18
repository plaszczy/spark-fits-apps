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


zcut=[0.9,1.1]
angpow='dc2_z1_smooth.fits'

df_map=df.filter(df.redshift.between(zcut[0],zcut[1])).select("ipix").groupBy("ipix").count()
p=df_map.toPandas()
Ntot=sum(p['count'])
Nmean=mean(p['count'])

print("Ntot={}M Nmean/pix={}".format(Ntot,Nmean))

skyMap= full(hp.nside2npix(nside),hp.UNSEEN)
skyMap[p['ipix'].values]=p['count'].values/Nmean-1.


Npix=150
#Ldeg=13.5
Ldeg=sqrt(pixarea)*Npix/60
print("L={} deg".format(Ldeg))
L=deg2rad(Ldeg)
L2=L*L

k0=2*pi/L
kmax=k0*Npix/2
print("k0={} kmax={}".format(k0,kmax))

c=hp.gnomview(skyMap,rot=rot,reso=reso,xsize=Npix,return_projected_map=True)
img=c.data

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

plt.figure()
plt.pcolormesh(freq,freq,psd,vmax=1e-6)
plt.colorbar()
plt.title("{}<z<{}".format(zcut[0],zcut[1]))
plt.xlabel(r"$\ell_x$")
plt.ylabel(r"$\ell_y$")
plt.tight_layout()


#1D##########
#
x,y=meshgrid(freq,freq)
kmap=sqrt(x**2+y**2)

#binning in k
kbin=arange(0,kmax,50)


kmean=[]
stdmean=[]
psmean=[]
stdps=[]
for k1,k2 in zip(kbin[0:-1],roll(kbin,-1)):
    w=where(logical_and(kmap>k1,kmap<k2))
    kmean.append(mean(kmap[w].flat))
    stdmean.append(std(kmap[w].flat))
    psmean.append(mean(psd[w].flat))
    stdps.append(std(psd[w].flat))

#shot noise (1/Nbar=4pi/Ntot=L2/Ntot)
clsn=4*pi/Ntot

#angpow

plt.figure()
t=tools.mrdfits(angpow,1)
plt.plot(t.ell,t.cl00,'r',label='AngPow')
plt.errorbar(kmean,array(psmean),yerr=stdps,xerr=25,fmt='o',c='k',label='cosmoDC2')
plt.xlabel(r"$\ell$")
plt.ylabel(r"$C_\ell$")
plt.title("{}<z<{}".format(zcut[0],zcut[1]))
tools.ax0()
plt.tight_layout()
