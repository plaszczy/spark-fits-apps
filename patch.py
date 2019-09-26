from numpy import *
from numpy.fft import fft2,fftshift,fftfreq

def patch_spectrum(c,L,bink=50):
    #c counts map (unscaled)
    #L en radian

    Nx,Ny=array(c).shape

    Ldeg=rad2deg(L)
    L2=L*L
    print("L={} deg".format(Ldeg))

    #modes
    k0=2*pi/L
    kmax=k0*min([Nx,Ny])/2
    print("k0={} kmax={}".format(k0,kmax))

    Nmean=c.mean()
    Ntot=c.sum()
    #rescale
    img=c/Nmean-1.

    dens=Ntot/Ldeg**2/3600
    print("mean density on patch={} gals/arcmin**2".format(dens))

    #in correct units
    Nbar=Ntot/L2
    print("estimated shot noise={}".format(1/Nbar))

    #2D
    #window
    wx=kaiser(Nx,10)
    wy=kaiser(Ny,10)

    x,y=meshgrid(wx,wy)
    hann=x*y
    fapo=fft2(hann)
    w2=sum(abs(fapo)**2)

    #fft
    F1 = fft2(img*hann)
    F2 = fftshift( F1 )
    psd=abs(F2)**2/w2*L2

    freqX=fftshift(fftfreq(Nx)*Nx*k0)
    freqY=fftshift(fftfreq(Ny)*Ny*k0)

    #1D##########
    #
    kx,ky=meshgrid(freqX,freqY)
    kmap=sqrt(kx**2+ky**2)

    #binning in k
    kbin=arange(0,kmax,bink)


    kmean=[]
    psmean=[]
    stdps=[]
    for k1,k2 in zip(kbin[0:-1],roll(kbin,-1)):
        w=where(logical_and(kmap>k1,kmap<k2))
        kmean.append(mean(kmap[w].flat))
        psmean.append(mean(psd[w].flat))
        stdps.append(std(psd[w].flat))
        
    return array(kmean),array(psmean)-1/Nbar,array(stdps)
