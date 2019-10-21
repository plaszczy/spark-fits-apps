
from pylab import *
from tools import *
from scipy.interpolate import interp1d


def histo_plot(x,y,label=""):
    figure()
    bar_outline(x,y)
    ylim(0.8*min(y),1.2*max(y))
    semilogy()
    axvline(0,c='k',lw=0.5)
    xlabel(label)

    f=y/sum(y)
    #moyenne
    xmean=sum(x*f)
    xx=x-xmean
    vx=sum(xx**2*f)

    imax=argmax(y)
    fup = interp1d(y[imax:],x[imax:])
    x1=fup(y[imax]/2)
    fd = interp1d(y[0:imax+1],x[0:imax+1])
    x2=fd(y[imax]/2)

    fwhm=float(x1)-float(x2)

    stat=["N={:d}".format(int(sum(y))),r"$\mu={:g}$".format(xmean),r"$\sigma={:g}$".format(sqrt(vx)),"fwhm={:g}".format(fwhm)]
    ax=gca()
    text(0.6,0.8,"\n".join(stat), horizontalalignment='left',transform=ax.transAxes)
    show()

x,y=loadtxt("df.txt",unpack=True)
histo_plot(x,y)


