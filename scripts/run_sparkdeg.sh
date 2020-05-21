#!/bin/bash

nargs=$#

if [ $nargs -lt 3 ]; then
echo "##################################################################################"
echo "usage: "
echo "./run_sparkdeg.sh sep[arcmin] zmax Nodes (t[min])"
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

sep=$1
zmax=$2
nodes=$3


export SPARKVERSION=2.4.4
IMG=registry.services.nersc.gov/plaszczy/spark_desc:v$SPARKVERSION

prefix="s${sep}_z${zmax}_N${nodes}"
slfile="run_$prefix.sl"
echo $slfile
cat > $slfile <<EOF
#!/bin/bash

#SBATCH -q $queue
#SBATCH -N $nodes
#SBATCH -C haswell
#SBATCH -t $t
#SBATCH -e slurm_${prefix}_%j.err
#SBATCH -o slurm_${prefix}_%j.out
#SBATCH --image=$IMG
#SBATCH --volume="/global/cscratch1/sd/$USER/tmpfiles:/tmp:perNodeCache=size=200G"

#init
source $HOME/desc-spark/scripts/init_spark.sh

#check spark3d
export EXEC_CLASSPATH=$HOME/SparkLibs
JARS=\$EXEC_CLASSPATH/jhealpix.jar,\$EXEC_CLASSPATH/spark-fits.jar,\$EXEC_CLASSPATH/spark3d.jar

shifter spark-shell $SPARKOPTS --jars \$JARS --conf spark.driver.args="${sep} ${zmax}" -I Timer.scala -i autocolore.scala

stop-all.sh

EOF


cat $slfile

sbatch $slfile
