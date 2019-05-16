import pandas as pd
import numpy as np
from galcols import *

#from importlib import reload
#reload(color)

dfp=pd.read_hdf("cosmodc2_excerpt.hdf5",key="cosmoDC2 v1.1.4")


r=dfp['Mag_true_r_lsst_z0']
g=dfp['Mag_true_g_lsst_z0']

c=np.exp(-(r-g)/2.5)
mm=(min(c),max(c))
h=hue(c,mm)

max_mag=np.maximum(abs(r),abs(g))
mm=(min(max_mag),max(max_mag))
val=value(max_mag,mm)
val=clip(val+0.5,0.5,1)

sat=np.ones_like(h)

pos=['position_x','position_y','position_z']

rgb=np.transpose(hsv2rgb(h,sat,val))

df=dfp[pos]
df['r']=pd.Series(rgb[0])
df['g']=pd.Series(rgb[1])
df['b']=pd.Series(rgb[2])

import inl
inl.plot3D_col(df.values,pointSize=5)
