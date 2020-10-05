

#construction carte nside defini
#filter
dfcut=df.filter(...)


var="convergence"
df_map=dfcut.select("ipix",var).groupBy("ipix").avg(var)

p=df_map.toPandas()
#sans rescale
skyMap= full(hp.nside2npix(nside),hp.UNSEEN)
skyMap[p['ipix'].values]=p[p.columns[1]].values


#gnome proj 
Npix=250
cen=[61.8,-37.2]

Ldeg=reso*Npix/60
L=deg2rad(Ldeg)
print("angsize={} deg".format(Ldeg))

c=hp.gnomview(skyMap,rot=cen,reso=reso,xsize=Npix,return_projected_map=True)
assert c.mask.sum()==0

img=c.data

from numpy.fft import fft2,fftshift,fftfreq
##analyse
g1=loadtxt("g1gnom.dat")
g2=loadtxt("g2gnom.dat")
conv=loadtxt("convgnom.dat")
g=vectorize(complex)(g1,g2)


#surdensite
#g1=g1/g1.mean()-1.
#g2=g2/g2.mean()-1.



#fuorier coeff
Nx,Ny=array(g1).shape

#nside=1024:
reso=3.435486

Ldeg=Nx*reso/60.
L=deg2rad(Ldeg)
L2=L*L
print("L={} deg".format(Ldeg))

#modes
k0=2*pi/L
kmax=k0*min([Nx,Ny])/2
print("k0={} kmax(1D)={}".format(k0,kmax))

#rm mean

#2D apo window
wx=kaiser(Nx,10)
wy=kaiser(Ny,10)

x,y=meshgrid(wx,wy)
hann=x*y

hannk=fft2(hann)

freqX=fftshift(fftfreq(Nx)*Nx*k0)
freqY=fftshift(fftfreq(Ny)*Ny*k0)

kx,ky=meshgrid(freqX,freqY)
modk=sqrt(kx**2+ky**2)

ct2k=(kx**2-ky**2)/modk**2
st2k=-(2*kx*ky)/modk**2

comp_t=vectorize(complex)(ct2k,st2k)


#ffts
F1 = fft2(g1*hann)
G1= fftshift( F1 )

F2 = fft2(g2*hann)
G2= fftshift( F2 )

G=fftshift(fft2(g*hann))

Ek=G1*ct2k+G2*st2k
Bk=-G1*st2k+G2*ct2k

#shift back
Ek1=ifftshift(Ek)
Ek1[0,0]=0
Ex=ifft2(Ek1)

Bk1=ifftshift(Bk)
Bx=ifft2(Bk1)

#invert complex map
Gs=ifftshift(G*comp_t) 
Gs[0,0]=0

figure()
imshow(real(fft2(Gs)),vmin=-2000,vmax=2000)
colorbar()
suptitle("E map")

figure()
imshow(imag(fft2(Gs)),vmin=-2000,vmax=2000)
colorbar()
suptitle("B map")
