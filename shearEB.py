

#construction carte nside defini
#filter
dfcut=df.filter(...)


var="shear_2"
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


