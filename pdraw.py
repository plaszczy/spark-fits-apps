
import pandas as pd

df=pd.read_hdf("strong1.hdf5")


#dtermine tragets
p.loc[p['halo_id']==3584900064124] ->691
p.loc[p['halo_id']==4950100064124]  ->693                                                            

#select
#df1=df.filter(items=["position_x","position_y","position_z"])
df1=df.filter(items=["ra","dec","redshift"])

#new col
f=np.ones(df.index.size,dtype='int')*255
f[691]=200 
f[693]=200

#append
df1 = df1.assign(type=pd.Series(f).values)


d1=sqrt((df['ra']-ra0)**2+(df['dec']-dec0)**2)*3600


df = df.assign(d1=pd.Series(d1).values)


ra1=50.133892
dec1= -31.16941

d2=sqrt((df['ra']-ra1)**2+(df['dec']-dec1)**2)*3600
df = df.assign(d2=pd.Series(d2).values)

df.sort_values('d1')   




ra0=50.129242
dec0= -31.16934

ra1=50.133892
dec1= -31.16941

scatter((df['ra']-ra0)*3600,(df['dec']-dec0)*3600)
axhline(0,c='k')
axvline(0,c='k')
scatter([0,(ra1-ra0)*3600],[0,(dec1-dec0)*3600],c='r') 
