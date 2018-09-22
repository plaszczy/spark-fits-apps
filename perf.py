from pylab import *
from tools import *

def reject_outliers(data, m = 3.):
    d = np.abs(data - np.median(data))
    mdev = np.median(d)
    s = d/(mdev if mdev else 1.)
    return data[s<m]



labels=["load(HDU)","PZ + show(5)","cache (count)","stat(z)","stat(all)","minmax(z)","histo dataframe","histo UDF","histo pandas UDF"]

        
tsca=genfromtxt("nersc/scala_perf.txt")
tsca=np.transpose(tsca)

tpy=genfromtxt("nersc/python_perf.txt")
tpy=np.transpose(tpy)

figure(figsize=(15,10))
for i in range(len(tpy)):
    subplot(3,3,i+1)
    d1=reject_outliers(tpy[i])
    m1=mean(d1)
    s1=std(d1)
    d2=reject_outliers(tsca[i])
    m2=mean(d2)
    s2=std(d2)
    hist([d1,d2],30)
    legend(["python: {:2.1f}+-{:2.1f}".format(m1,s1),"scala: {:2.1f}+-{:2.1f}".format(m2,s2)])
    xlabel(labels[i])
    print(r"{} & ${:2.1f}\pm{:2.1f}$ & ${:2.1f}\pm{:2.1f}$\\".format(labels[i],m1,s1,m2,s2))

tight_layout()
show()
