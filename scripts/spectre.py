
from numpy.fft import fft2,fftshift,fftfreq
import pandas as pd

zcut=[0.9,1.1]

df_map=df.filter(df.redshift.between(zcut[0],zcut[1])).select("ipix").groupBy("ipix").count()
p=df_map.toPandas()

Nbar=mean(p['count'])
print("Nbar={}".format(Nbar))
skyMap= full(hp.nside2npix(nside),hp.UNSEEN)
skyMap[p['ipix'].values]=p['count'].values/Nbar-1.

N=150
#Ldeg=13.5
Ldeg=sqrt(pixarea)*N/60
print("L={} deg".format(Ldeg))
L=deg2rad(Ldeg)
L2=L*L

k0=2*pi/L
kmax=k0*N/2*sqrt(2)
print("k0={} kmax={}".format(k0,kmax))

c=hp.gnomview(skyMap,rot=rot,reso=reso,xsize=N,return_projected_map=True)
img=c.data

#2D
#window
nx=kaiser(N,10)

x,y=meshgrid(nx,nx)
hann=x*y
fapo=fft2(hann)
w2=sum(abs(fapo)**2)

#fft
F1 = fft2(img*hann)
F2 = fftshift( F1 )
psd=abs(F2)**2/w2*L2

freq=fftshift(fftfreq(N)*N*k0)

plt.figure()
plt.pcolormesh(freq,freq,psd,vmax=0.0001)
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
kbin=arange(0,kmax-10,50)


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
clsn=4*pi/sum(p['count'])

plt.figure()
plt.errorbar(kmean,array(psmean),yerr=stdps,xerr=stdmean,fmt='+')
plt.xlabel(r"$\ell$")
plt.ylabel(r"$C_\ell$")
plt.title("{}<z<{}".format(zcut[0],zcut[1]))
plt.axhline(0,c='k')
plt.axhline(clsn)
plt.tight_layout()

#retirer le SN pour comparer a angpow

