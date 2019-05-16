import pandas as pd
import numpy as np



import colorsys
hsv2rgb=np.vectorize(colorsys.hsv_to_rgb)


def clip(v,xmin,xmax):
    w=v<xmin
    if len(w)>0:
        v[w]=xmin
    w=v>xmax
    if len(w)>0:
        v[w]=xmax
    return v
    
def hue(x,mm):
    v=240*(mm[1]-x)/(mm[1]-mm[0])
    return clip(v,0,240)

def value(x,mm):
    v=(x-mm[0])/(mm[1]-mm[0])
    return(clip(v,0,1))
    

if __name__ == '__main__':

    dfp=pd.read_hdf("cosmodc2_excerpt.hdf5",key="cosmoDC2 v1.1.4")

    u=dfp['Mag_true_u_lsst_z0']  
    g=dfp['Mag_true_g_lsst_z0']
    r=dfp['Mag_true_r_lsst_z0'] 
    i=dfp['Mag_true_i_lsst_z0']
    z=dfp['Mag_true_z_lsst_z0']
    y=dfp['Mag_true_Y_lsst_z0']

    #fluxes
    c=np.exp(-(r-g)/2.5)
    mm=(min(c),max(c))

    h=hue(c,mm)

    max_mag=np.maximum(abs(r),abs(g))
    mm=(min(max_mag),max(max_mag))
    val=value(max_mag,mm)
    val=clip(val+0.5,0.5,1)

    sat=np.ones_like(h)

    rgb=np.transpose(hsv2rgb(h,sat,val))

    import inl
    inl.plot3D(rgb,width=700,height=700)
