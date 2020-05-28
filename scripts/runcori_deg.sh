#!/bin/bash

declare -i nargs
nargs=$#

if [ $nargs -lt 2 ]; then
echo "##################################################################################"
echo "usage: "
echo "./run_sparkdeg.sh sep Nodes (#iterations)"
echo "##################################################################################"
exit
fi


sep=$1
nodes=$2
part=$(($nodes*32))

#optional
nit=1
if [ $# -eq 3 ] ; then
nit=$3
fi

zmax=1.025


export SPARKVERSION=2.4.4
IMG=registry.services.nersc.gov/plaszczy/spark_desc:v$SPARKVERSION

prefix="s${sep}_z${zmax}_N${nodes}"
slfile="run_$prefix.sl"
echo $slfile
cat > $slfile <<EOF
#!/bin/bash

#SBATCH -q debug
#SBATCH -t 00:05:00
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

#export FITSSOURCE="/global/cscratch1/sd/plaszczy/LSST10Y"
export INPUT="/global/cscratch1/sd/plaszczy/tomo100M.parquet"

#pas de coalesce
shifter spark-shell $SPARKOPTS --jars \$JARS --conf spark.driver.args="${sep} ${zmax} ${part}" -I hputils.scala -I Timer.scala -i autocoloreXYZ.scala

stop-all.sh

EOF


cat $slfile
for it in $(seq $nit)
do
    echo $it
    sbatch $slfile
done
