#!/bin/bash

declare -i nargs
nargs=$#

if [ $nargs -lt 3 ]; then
echo "##################################################################################"
echo "usage: "
echo "./run_sparkdeg.sh sep zmax Nodes (#iterations)"
echo "##################################################################################"
exit
fi

#optional
declare -i nit
nit=1
if [ $# -eq 4 ] ; then
nit=$4
fi

sep=$1
zmax=$2
nodes=$3
ncores=$(($nodes*32))

export SPARKVERSION=2.4.4
IMG=registry.services.nersc.gov/plaszczy/spark_desc:v$SPARKVERSION

prefix="s${sep}_z${zmax}_N${nodes}"
slfile="run_$prefix.sl"
echo $slfile
cat > $slfile <<EOF
#!/bin/bash

#SBATCH -q debug
#SBATCH -t 00:10:00
#SBATCH -N $nodes
#SBATCH -C haswell
#SBATCH -e slurm_${prefix}_%j.err
#SBATCH -o slurm_${prefix}_%j.out
#SBATCH --image=$IMG
#SBATCH --volume="/global/cscratch1/sd/$USER/tmpfiles:/tmp:perNodeCache=size=200G"
printenv | grep SPARK

#init
source $HOME/desc-spark/scripts/init_spark.sh

#jars
LIBS=$HOME/SparkLibs
JARS=\$LIBS/jhealpix.jar,\$LIBS/spark-fits.jar,\$LIBS/spark3d.jar

shifter spark-shell $SPARKOPTS --jars \$JARS --conf spark.driver.args="${sep} ${zmax} ${ncores}" -I hputils.scala -I Timer.scala -i autocolore.scala

stop-all.sh

EOF


cat $slfile
for it in $(seq $nit)
do
    echo $it
    sbatch $slfile
done
