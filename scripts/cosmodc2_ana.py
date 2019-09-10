
#hitso mags
for (b,c) in zip(bands,colors):
    h,step=df_hist(df,"mag_{}".format(b),bounds=(15,40))
    #plt.bar(h['loc'],h['count'],step,label=b,color='white',edgecolor=c)
    plt.plot(h['loc'],h['count'],color=c,label=b)
    
plt.legend()

#magnification
df_lens=df.withColumn("mag_lens-mag_true",df.mag_u-df.mag_true_u)
df_histplot(df_lens,"mag_lens-mag_true",bounds=(-0.4,0.5),Nbins=80,doStat=True)



#strong lensing?
df_lens.filter(df_lens["mag_lens-mag_true"]<-2.5).show()


#maps
#m=df.select(F.mean("ra"),F.mean("dec")).first()
#mean patch
rot=[61.81579482165925,-35.20157446022967]


#dfp=df.filter(df.ipix==38188148)
#dfp=dfp.withColumn('shift',dfp.mag_u-dfp.mag_true_u)
#p=dfp.toPandas()
#p_h=p[p['shift']<-1]

#strong lensing
rot=[50.133892,-31.16941]


#nside=4096:
 df.filter(df.magnification>5).groupby("ipix").count().show()
+---------+-----+                                                               
|     ipix|count|
+---------+-----+
|152742122|   24|
|172411589|   44|
|172381162|    7|
|172396377|   70|
|172396378|   26|
|152758506|    2|
|152774890|    1|
|152758505|   30|
|152742121|   34|
|152758504|   11|
|152725737|    1|
+---------+-----+


#find hotspots
pix= df.filter(df.magnification>3).groupby("ipix").count().toPandas()
radec=hp.pix2ang(4096,pix.ipix.values,lonlat=True)
np.sort(radec[0])


hp.gnomview(m,rot=[50.14160156,-31.15903841],xsize=100,cmap='hot')
