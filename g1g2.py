from pylab import *
import pandas
from tools import *

#1ds
p1=pandas.read_csv("g1.csv")
x=p1['loc'].values
bs1=(roll(x,-1)-x)[0]
h=p1['count'].values
g1=h/sum(h)/bs1
mean1=bs1*sum(x*g1)
print("mean1={}".format(mean1))
sig1=sqrt(bs1*sum((x-mean1)**2*g1))
print("sig1={}".format(sig1))

p2=pandas.read_csv("g2.csv")
y=p2['loc'].values
bs2=(roll(y,-1)-y)[0]
h=p2['count'].values
g2=h/sum(h)/bs2
mean2=bs2*sum(y*g2)
print("mean2={}".format(mean2))
sig2=sqrt(bs2*sum((y-mean2)**2*g2))
print("sig2={}".format(sig2))

#2D
g12=loadtxt("g12.txt")
g12=g12/sum(g12)/bs1/bs2

#cov
cov=0.
for i in range(len(x)):
    cov+=bs1*bs2*sum(x[i]*y*g12[i,:])

cor=cov/sig1/sig2

#0.005461296333521547

print("cor={}".format(cor))
#marginals
marg1=bs2*sum(g12,0)
marg2=bs1*sum(g12,1)

meanmarg1=bs1*sum(x*marg1)
meanmarg2=bs2*sum(x*marg2)
print("mean marg1={}".format(meanmarg1))
print("mean marg2={}".format(meanmarg2))


imshowXY(x,y,g12) 
xlabel("g1")
ylabel("g2")
suptitle("f(g1,g2)")

G1,G2=meshgrid(g1,g2)
a=g12/(G1*G2)

imshowXY(x,y,a) 
suptitle("f(g1,g2)/(f(g1)f(g2))")

show()


E12=[bs1*sum(x*g12[i,:]/g2[i]) for i in range(len(x))]


cond2=g12/G2
imshowXY(x,y,cond2) 
suptitle("f(g1,g2)/f(g2)")
xlabel("g1")
ylabel("g2")



#conditionnel 2d
c2=a*G1 
imshowXY(x,y,cond2) 
suptitle("f(g1,g2)/f(g2)")
xlabel("g1")
ylabel("g2")


#mean condtionnelle
figure()
ec2=[bs1*sum(x*c2[i,:]) for i in range(len(x))]
xlabel("g1")
ylabel("E[g1|g2]")
plot(x,ec2)
tight_layout()
g12#qq slices
figure()
[plot(x,c2[i,:],label="g2={:.3f}".format(x[i])) for i in range(50,100,10)] 
xlabel("g1")
ylabel("f(g1|g2)")
legend()
