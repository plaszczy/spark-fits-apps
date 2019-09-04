import pandas as pd
import numpy as np
from astropy.visualization import make_lupton_rgb
from astropy import units as u
from tools import *
import inl

def bin_and_sort(x,N,bounds=None):
    r=superclip(x,0,1,bounds)
 # on veut de 1 a N+1
    bin=(r*N+1).astype(int)
    iord=[]
    for i in range(1,N+1):
        iord.append(where(bin==i)[0].tolist())
    return bin,iord

#
dfp=pd.read_hdf("cosmodc2_excerpt.hdf5",key="cosmoDC2 v1.1.4")

pos=['position_x','position_y','position_z']
df=dfp[pos]

#color
# i->r, r->g, g->b

#AB mag
mag_i=dfp['Mag_true_i_lsst_z0'].values
mag_r=dfp['Mag_true_r_lsst_z0'].values
mag_g=dfp['Mag_true_g_lsst_z0'].values

#flux (Jy)
flux_i = (mag_i*u.ABmag).to(u.Jy).value

flux_r = (mag_r*u.ABmag).to(u.Jy).value

flux_g = (mag_g*u.ABmag).to(u.Jy).value


print(df.columns)

#lupton->scale to [0,1]
rgb_arr = make_lupton_rgb(flux_i,flux_r,flux_g)
rgb_256=transpose(rgb_arr[0])
rgb=array(rgb_256,dtype=float)/256

df=df.assign(r=rgb[0])
df=df.assign(g=rgb[1])
df=df.assign(b=rgb[2])

#heat values
heat=abs(mag_i)-abs(mag_g)
#df=df.assign(heat=heat)

#size##########################
sz=dfp['size_true']
q=percentile(sz,[1,99])
Nsz=10
b,iord=bin_and_sort(sz,Nsz,bounds=q)
# hist_plot(b,bins=100)
#size range [1,10]
size_bins=superclip(arange(Nsz),1,10)

print(df.columns)

##plots
#inl.plot3D_heat(df.values,pointSize=1)
#inl.plot3D_rgb(df.values,rgb_index=(3,4,5),pointSize=1)
#inl.plot3D_size_heat(df.values,iord,bin_size=size_bins)

