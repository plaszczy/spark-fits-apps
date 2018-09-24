from pylab import *

label=["read+cache","statistics","df(native)","df(+udf)","tomo (1 bin)"]

upsud_py=[113.,9.8,11.5,43,30]
upsud_sca=[117.,11,13,13.9,13]


nersc_py=[87.7,8.6,16.2,43,40]
nersc_sca=[92,8.5,15.1,15.3]


nersc2_py=[50,5.6,7.8,27,29.5]
nersc2_sca=[54,6.5,11.6,8.3]


x=array([0,2,4,6,8])

figure()
bar(x-0.2, upsud_py,width=0.2,color='b',align='center',tick_label=label)
bar(x, nersc_py,width=0.2,color='g',align='center')
bar(x+0.2, nersc2_py,width=0.2,color='r',align='center')
ylabel("user time (s)")
ylim(0,120)

title("python")
legend(["UPSUD","NERSC=UPSUD","NERSC=UPSUD*2"])


## figure()
## bar(x-0.2, upsud_sca,width=0.2,color='b',align='center',tick_label=label)
## bar(x, nersc_sca,width=0.2,color='g',align='center')
## bar(x+0.2, nersc2_sca,width=0.2,color='r',align='center')

## title("scala")
## legend(["UPSUD","NERSC=UPSUD","NERSC=UPSUD*2"])

show()

