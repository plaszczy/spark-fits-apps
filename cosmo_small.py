import pandas as pd
import numpy as np
from astropy.visualization import make_lupton_rgb
from astropy import units as u

def superclip(x,a,b,bounds=None):
	if bounds is None:
		bounds=[np.min(x),np.max(x)]
	r=(x-bounds[0])/(bounds[1]-bounds[0])
	v=a+(b-a)*r
	return np.clip(v,a,b)

def bin_and_sort(x,N,bounds=None):
    r=superclip(x,0,1,bounds)
 # on veut de 1 a N+1
    bin=(r*N+1).astype(int)
    iord=[]
    for i in range(1,N+1):
        iord.append(where(bin==i)[0].tolist())
    return bin,iord


R=np.array([1,0,0])
B=np.array([0,0,1])
def rgb_col(b,r,dyn_range=0.5):
    S=np.array([b,b,b])
    if r>0 :
        global R
        SR=R-S
        return S+r*SR*dyn_range
    else:
        global B
        SB=B-S
        return S+abs(r)*SB*dyn_range

#
dfp=pd.read_hdf("cosmodc2_excerpt.hdf5",key="cosmoDC2 v1.1.4")

m_r=dfp['Mag_true_r_lsst_z0']
m_b=dfp['Mag_true_g_lsst_z0']

#brightness
m_max=np.minimum(m_r,m_b)
m_ref=np.median(m_max)
abs_flux=10**(-0.4*(m_max-m_ref))
bri=superclip(abs_flux,0.8,1,np.percentile(abs_flux,[5,95]))

#color
r_flux=10**(-0.4*(m_r-m_b))
RoB=superclip(r_flux,-1,1,np.percentile(r_flux,[1,99]))

rgb=[]
[rgb.append(rgb_col(b,r)) for b,r in zip(bri,RoB)]
rgb=np.transpose(rgb)

heat=abs(m_r)-abs(m_b)
pos=['position_x','position_y','position_z']

#pos=['position_x','position_y','position_z','size_true']

df=dfp[pos]
df=df.assign(heat=r_flux)

print(df.columns)

#df=df.assign(g=rgb[1])
#df=df.assign(b=rgb[2])


#lupton
mag_i=dfp['Mag_true_i_lsst_z0'].values
mag_r=dfp['Mag_true_r_lsst_z0'].values
mag_g=dfp['Mag_true_g_lsst_z0'].values

flux_g = (mag_g*u.ABmag).to(u.nJy).value
flux_r = (mag_r*u.ABmag).to(u.nJy).value
flux_i = (mag_i*u.ABmag).to(u.nJy).value
rgb_arr = make_lupton_rgb(flux_i,flux_r,flux_g)
rgb_256=transpose(rgb_arr[0])
rgb=array(rgb_256,dtype=float)/256

df=df.assign(r=rgb[0])
df=df.assign(g=rgb[1])
df=df.assign(b=rgb[2])


import inl
#inl.plot3D_heat(df.values,pointSize=1,width=1200,height=1000)
inl.plot3D_rgb(df.values,pointSize=1)


#size##########################
sz=dfp['size_true']
q=percentile(sz,[1,99])

Nsz=50
b,iord=bin_and_sort(sz,Nsz,bounds=q)
# hist_plot(b,bins=100)

size_bins=superclip(arange(Nsz),1,10)


inl.plot3D_size_heat(df.values,iord,bin_size=size_bins,width=1200,height=900)

