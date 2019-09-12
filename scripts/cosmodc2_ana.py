
#maps
#m=df.select(F.mean("ra"),F.mean("dec")).first()
#mean patch
rot=[61.81579482165925,-35.20157446022967]


#hitso mags
#for (b,c) in zip(bands,colbands):

for b in ['y']:
    plt.figure()
    h,step=df_hist(df,"mag_{}".format(b),bounds=(15,40))
    h_uf,step=df_hist(df.filter(df.halo_id<0),"mag_{}".format(b),bounds=(15,40))
    h_gal,step=df_hist(df.filter(df.halo_id>0),"mag_{}".format(b),bounds=(15,40))
    plt.bar(h['loc'],h['count'],step,label="total",color='b')
    plt.bar(h_uf['loc'],h_uf['count'],step,label="ultra faint",color='g')
    plt.bar(h_gal['loc'],h_gal['count'],step,label="gal",color='r',alpha=0.5)
    plt.xlabel("mag_{}".format(b))
    plt.legend()

#magnification
df_lens=df.withColumn("mag_lens-mag_true",df.mag_u-df.mag_true_u)
df_histplot(df_lens,"mag_lens-mag_true",bounds=(-0.4,0.5),Nbins=80,doStat=True)


#dfp=df.filter(df.ipix==38188148)
#dfp=dfp.withColumn('shift',dfp.mag_u-dfp.mag_true_u)
#p=dfp.toPandas()
#p_h=p[p['shift']<-1]

#strong lensing

#find hotspots
pix= df.filter(df.magnification>3).groupby("ipix").count().toPandas()
radec=hp.pix2ang(4096,pix.ipix.values,lonlat=True)
np.sort(radec[0])

hp.gnomview(m,rot=[50.14160156,-31.15903841],xsize=100,cmap='hot')

#colormag

