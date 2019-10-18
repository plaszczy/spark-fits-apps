
from pylab import *
from tools import *
from scipy.interpolate import interp1d

x,y=loadtxt("dphi.txt",unpack=True)
figure()
bar_outline(x,y)
ylim(1,1.2*max(y))
semilogy()
axvline(0,c='k',lw=0.5)
xlabel(r"$\Delta\Phi\quad(arcsec)$")

f=y/sum(y)

#moyenne
xmean=sum(x*f)
xx=x-xmean
vx=sum(xx**2*f)

fup = interp1d(y[x>xmean],x[x>xmean])
x1=fup(max(y)/2)
fd = interp1d(y[x<xmean],x[x<xmean])
x2=fd(max(y)/2)

fwhm=float(x1)-float(x2)


stat=["N={:d}".format(int(sum(y))),r"$\mu={:g}$".format(xmean),r"$\sigma={:g}$".format(sqrt(vx)),"fwhm={:g}".format(fwhm)]
ax=gca()
text(0.6,0.8,"\n".join(stat), horizontalalignment='left',transform=ax.transAxes)

show()
