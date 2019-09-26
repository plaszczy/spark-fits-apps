from numpy import *
import pandas as pd
import tools
import healpy as hp
from patch_spectrum import patch_spectrum


##creation map


## tomo
## zmean=0.75
## dz=0.25
## zcut=[zmean-dz,zmean+dz]
## angpow="dc2_z{}_smooth.fits".format(zmean)


#construction carte nside defini
#filter
dfcut=df.filter(...)

df_map=dfcut.select("ipix").groupBy("ipix").count()

p=df_map.toPandas()
#sans rescale
skyMap= full(hp.nside2npix(nside),hp.UNSEEN)
skyMap[p['ipix'].values]=p['count'].values


#gnome proj 
Npix=110
L=deg2rad(sqrt(pixarea)*Npix/60)


k0=2*pi/L
kmax=k0*Npix/2
print("k0={} kmax={}".format(k0,kmax))


#proj dans rot
c=hp.gnomview(skyMap,rot=rot,reso=reso,xsize=Npix,return_projected_map=True)


assert c.mask.sum()==0

k,ps,stdps=patch_spectrum(c.data,L)


plt.figure()
t=tools.mrdfits(angpow,1,silent=True)
plt.plot(t.ell,t.cl00,'r',label='AngPow')
plt.errorbar(k,ps,yerr=stdps,xerr=25,fmt='o',c='k',label='run2.1.1i')
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
plt.errorbar(kmean,(psmean-1/Nbar)/ck,yerr=stdps/ck,xerr=25,fmt='o',c='k',label='bias')
plt.xlabel(r"$\ell$")
plt.ylabel(r"$\hat C_\ell/C_\ell^{th}$")
plt.title("{}<z<{}".format(zcut[0],zcut[1]))
plt.tight_layout()

b2=(psmean[1:]-1/Nbar)/ck[1:]
sigb2=stdps[1:]/ck[1:]
wi=1/sigb2**2
meanb2=sum(wi*b2)/sum(wi)
sigmeanb2=sqrt(1/sum(wi))


plt.fill_between([0,kmax],[meanb2-sigmeanb2,meanb2-sigmeanb2],[meanb2+sigmeanb2,meanb2+sigmeanb2],color='green',alpha=0.5)

txt=r"$b^2={:2.2f}\pm{:2.2f}$".format(meanb2,sigmeanb2)
plt.text(0.1,0.3,txt,transform=plt.gca().transAxes,color='green', fontsize=12)
