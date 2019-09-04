
from pylab import *

def disp_cmap(cmap):
    cmap_size = cmap.size()
    print("heat size={}".format(cmap_size))

    figure()
    for icolor in range(cmap_size):
        SOPI_color = cmap.get_color(icolor)
        r = SOPI_color.r()
        g = SOPI_color.g()
        b = SOPI_color.b()
        print(r,g,b)
        bar(icolor,1,1,color=(r,g,b))
