
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
rot=[61.81579482165925,-35.20157446022967]
