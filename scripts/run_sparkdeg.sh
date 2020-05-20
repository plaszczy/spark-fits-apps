#!/bin/bash

nargs=$#
echo $nargs

if [ $nargs -lt 3 ]; then
echo "##################################################################################"
echo "usage: "
echo "./run_sparkdeg.sh zmax sep[arcmin] #nodes t[min](def=10)"
echo "##################################################################################"
exit
fi

#optional
declare -i t
t=10
if [ $# -eq 4 ] ; then
t=$4
fi

if [ $t -gt 30 ]; then
queue=interactive
else
queue=debug
fi


zmax=$1
sep=$2
nodes=$3


export SPARKVERSION=2.4.4
IMG=registry.services.nersc.gov/plaszczy/spark_desc:v$SPARKVERSION

cat > run_z$1_sep$2.sh <<EOF
#script writing
#!/bin/bash

#SBATCH -q $queue
#SBATCH -N $nodes
#SBATCH -C haswell
#SBATCH -t $t
#SBATCH -e spark_z$1_sep$2_%j.err
#SBATCH -o spark_z$1_sep$2_%j.out
#SBATCH --image=$IMG
#SBATCH --volume="/global/cscratch1/sd/$USER/tmpfiles:/tmp:perNodeCache=size=200G"

#init
source $HOME/desc-spark/scripts/init_spark.sh

#check spark3d
export EXEC_CLASSPATH=$HOME/SparkLibs
JARS=jhealpix.jar,spark-fits.jar,spark3d_2.11-0.3.1.jar

#decode
shifter spark-shell $SPARKOPTS --jars \$JARS --conf spark.driver.args="$zmax $sep" < autocolore.scala

stop-all.sh

EOF


cat run_z$1_sep$2.sh
