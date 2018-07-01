from numpy import *
from astropy.io import fits
import sys,os,glob
from time import time

class Catalog:
    def __init__(self,fname):
        data=(fits.open(fname)[1]).data
        print("reading {}".format(fname))
        self.ra=data['RA']
        self.dec=data['DEC']
        self.z=data['Z_COSMO']
        self.z+=data['DZ_RSD']
        self.zrec=self.z+random.randn(len(self.z))*0.03*(1+self.z)
        print("done")
    
class Timer:
    """
    a simple class for printing time (s) since last call
    """
    def __init__(self):
        self.t0=time()
        self.dt=0.

    def step(self):
        t1=time()
        self.dt=t1-self.t0
        self.t0=t1
        return self.dt

    def print(self,ana):
        print("-"*30)
        print(ana+"& {:2.1f} &".format(self.dt))
        print("-"*30)

files=glob.glob(sys.argv[1]+"/*.fits")
mins=[]
maxs=[]
timer=Timer()
for file in files:
    cat=Catalog(file)
    mins.append(min(cat.zrec))
    maxs.append(max(cat.zrec))
    timer.step()
    timer.print("minmax")

print("min={} max={}".format(min(array(mins)),max(array(maxs))))
timer.step()
timer.print("minmax")

    
