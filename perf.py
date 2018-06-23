from pylab import *
from tools import *

def reject_outliers(data, m = 2.):
    d = np.abs(data - np.median(data))
    mdev = np.median(d)
    s = d/(mdev if mdev else 1.)
    return data[s<m]



labels=["load(HDU)","PZ + show(5)","cache (count)","stat(z)","stat(all)","minmax(z)","histo dataframe","histo UDF","histo pandas UDF"]

        
tsca=genfromtxt("scala_perf.txt")
tsca=np.transpose(tsca)

tpy=genfromtxt("python_perf4.txt")
tpy=np.transpose(tpy)

figure(figsize=(15,10))
for i in range(len(tpy)):
    subplot(3,3,i+1)
    hist([tpy[i],tsca[i]],30,label=labels[i])
    legend()
    print(r"{} & ${:2.1f}\pm{:2.1f}$ & ${:2.1f}\pm{:2.1f}$\\".format(labels[i],tpy[i].mean(),tpy[i].std(),tsca[i].mean(),tsca[i].std()))

tight_layout()
show()
