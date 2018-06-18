from pylab import *
from tools import *

def reject_outliers(data, m = 2.):
    d = np.abs(data - np.median(data))
    mdev = np.median(d)
    s = d/(mdev if mdev else 1.)
    return data[s<m]




t=genfromtxt("scala_perf.txt.save")
t=transpose(t)

figure(figsize=(15,10))
for i in range(len(t)):
    subplot(2,5,i+1)
    data=reject_outliers(t[i],3)
    hist_plot(data,label="{}".format(i))
    print(r"{}: ${:2.1f}\pm{:2.1f}$".format(i+1,data.mean(),data.std()))

show()
