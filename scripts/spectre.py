from pylab import *
import pandas as pd

img=array(pd.read_hdf("overdensity.hdf5")) 
N=200

Ldeg=13.5
L=deg2rad(Ldeg)
area=L*L

k0=2*pi/L

#window
nx=hanning(N)
ny=hanning(N)
x,y=meshgrid(nx,ny)
hann=x*y
fapo=fft2(hann)
w2=np.sum(abs(fapo)**2)

#fft
F1 = fft2(img*hann)
F2 = fftshift( F1 )
psd=abs(F2)**2/w2/area

#modk map
freq=fftshift(fftfreq(N)*N*k0)
x,y=meshgrid(freq,freq)
kmap=sqrt(x**2+y**2)

#binning in k
kbin=arange(0,3000,30)

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

errorbar(kmean,psmean,yerr=stdps,xerr=stdmean,fmt='+')
