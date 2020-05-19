from astropy.coordinates import SkyCoord
import astropy.units as u
import numpy as np
from tools import *

sepcut=10*u.arcmin

print(sepcut)

nb = 1000

#deg
width=3*u.deg

ra_cen=(2*rand()-1)*90
ra_min=max(-90,ra_cen-width.value/2)
ra_max=min(90,ra_cen+width.value/2)

dec_cen=rand()*360.
dec_min = max(0,dec_cen-width.value/2)
dec_max = min(360,dec_cen+width.value/2)

print("center=[{},{}], width={}".format(ra_cen,dec_cen,width))

ra = np.random.uniform(ra_min, ra_max, nb)
xmin = (np.sin(dec_min*np.pi/180.)+1)/2.
xmax = (np.sin(dec_max*np.pi/180.)+1)/2.
dec = (np.arccos(2*np.random.uniform(xmin, xmax, nb)-1) - np.pi/2.) / np.pi * 180.

c= SkyCoord(ra, dec, unit=u.deg, frame='icrs')

pairs=c.search_around_sky(c, seplimit=sepcut)

i1=pairs[0]
i2=pairs[1]
d2=pairs[2].to(u.radian).value

#remove self values
w=d2>0
i1=i1[w]
i2=i2[w]
d2=d2[w]

edge=np.array([i1,i2]).T

deg=np.zeros(nb,dtype='int')
id=np.arange(nb)

for i in id:
    w1=(i1==i).sum()
    w2=(i2==i).sum()
    assert w1==w2
    deg[i]=w1

clf()
hist_plot(deg)
show()

from pandas import DataFrame
d = {"id":id,'ra': ra, 'dec': dec,"degtrue":deg}
df = DataFrame(data=d)

df.to_parquet("deg.parquet",engine='fastparquet',compression='gzip')
print("deg.parquet written")

#edges 0:
#w=(i1==0)
#e=array([i1[w],i2[w]]).T

#from healpy import *
#nside=2**(arange(10)+3) 
#resol=nside2resol(nside,arcmin=True) 
#d={"nside":nside,"Lpix":resol}
##    nside     L(arcmin)
## 0      8  439.742261
## 1     16  219.871130
## 2     32  109.935565
## 3     64   54.967783
## 4    128   27.483891
## 5    256   13.741946
## 6    512    6.870973
## 7   1024    3.435486
## 8   2048    1.717743
## 9   4096    0.858872

#nside>1/sepcut.to("rad")

#sepcut=logspace(log10(2.5),log10(250)) 
 ## array([  2.5       ,   2.74635285,   3.0169816 ,   3.31427841,
 ##         3.64087119,   3.9996468 ,   4.39377656,   4.82674432,
 ##         5.30237722,   5.82487953,   6.39886981,   7.02942174,
 ##         7.72210899,   8.48305443,   9.3189843 ,  10.23728766,
 ##        11.24608167,  12.3542834 ,  13.5716886 ,  14.90905829,
 ##        16.37821392,  17.99214183,  19.76510803,  21.71278434,
 ##        23.85238691,  26.20282835,  28.78488498,  31.62138042,
 ##        34.73738736,  38.16044918,  41.92082342,  46.05174923,
 ##        50.58974119,  55.57491206,  61.05132736,  67.06739488,
 ##        73.67629256,  80.93643857,  88.91200766,  97.67349843,
 ##       107.2983565 , 117.87165909, 129.48686698, 142.24665073,
 ##       156.26379813, 171.66221125, 188.57800158, 207.16069321,
 ##       227.5745445 , 250.        ])

