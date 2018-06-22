from pylab import *
from tools import *

def reject_outliers(data, m = 2.):
    d = np.abs(data - np.median(data))
    mdev = np.median(d)
    s = d/(mdev if mdev else 1.)
    return data[s<m]



labels=["load(HDU)","PZ + show(5)","cache (count)","statistics z","statistics all","minmax","histo df","histo UDF","histo pandas UDF"]

        
tsca=genfromtxt("scala_perf.txt.save")
tsca=np.transpose(tsca)

tpy=genfromtxt("python_perf.txt.save")
tpy=np.transpose(tpy)

figure(figsize=(15,10))
for i in range(len(tpy)):
    subplot(3,3,i+1)
    #data=reject_outliers(t[i],5)
    hist([tpy[i],tsca[i]],30)
    title(labels[i])
    print(r"{}: ${:2.1f}\pm{:2.1f}$ & ${:2.1f}\pm{:2.1f}$".format(i+1,tpy[i].mean(),tpy[i].std(),tsca[i].mean(),tsca[i].std()))

tight_layout()
show()
