
from pylab import *
from tools import *
from scipy.interpolate import interp1d


def histo_file(fn="df.txt",label="",newfig=True):
    b,x,y=loadtxt(fn,unpack=True)
    if newfig:
        figure()
    bar_outline(x,y)
    ylim(0.8*min(y),1.2*max(y))
    semilogy()
    axvline(0,c='k',lw=0.5)
    xlabel(label)

    #stats
    f=y/sum(y)
    xmean=sum(x*f)
    xx=x-xmean
    vx=sum(xx**2*f)
    sig=sqrt(vx)
    S=sum((xx/sig)**3*f)

    K=sum((xx/sig)**4*f)-3

    imax=argmax(y)

    xup=x[imax:]
    yup=y[imax:]
    x1=x[imax]
    if (len(xup)>5) :
        fup = interp1d(y[imax:],x[imax:])
        x1=fup(y[imax]/2)
    xd=x[0:imax+1]
    yd=y[0:imax+1]
    x2=x[imax]
    if (len(xd)>5) :
        fd = interp1d(y[0:imax+1],x[0:imax+1])
        x2=fd(y[imax]/2)

    fwhm=float(x1)-float(x2)

    stat=["N={:d}".format(int(sum(y))),"mode={:g}".format(x[imax]),"mean={:g}".format(xmean),"stdev={:g}".format(sqrt(vx)),"fwhm={:g}".format(fwhm),r"skew={:g}".format(S),r"kurt={:g}".format(K)]
    ax=gca()
    text(0.7,0.7,"\n".join(stat), horizontalalignment='left',transform=ax.transAxes)

    show()
    return b,x,y
