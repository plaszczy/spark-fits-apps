#!/bin/bash

nargs=$#

if ! [ $nargs -eq 2 ]; then
echo "##################################################################################"
echo "usage: "
echo "./run_sparkdeg.sh zmax sep[arcmin] #nodes"
echo "##################################################################################"
exit
fi




zmax=$1
sep=$2

nodes=$3
t="00:10:00"


export SPARKVERSION=2.4.4
IMG=registry.services.nersc.gov/plaszczy/spark_desc:v$SPARKVERSION

cat > run_z$1_sep$2.sh <<EOF
#script writing
#!/bin/bash

#SBATCH -q debug
#SBATCH -N $nodes
#SBATCH -C haswell
#SBATCH -t $t
#SBATCH -e spark_z$1_sep$2_%j.err
#SBATCH -o spark_z$1_sep$2_%j.out
#SBATCH --image=$IMG
#SBATCH --volume="/global/cscratch1/sd/$USER/tmpfiles:/tmp:perNodeCache=size=200G"

#init
source $HOME/desc-spark/scripts/init_spark.sh

#trasmit variable
export zmax=$zmax
export arcmin=$arcmin

shifter spark-shell $SPARKOPTS --jars $JARS --conf spark.driver.args="$sep" < autocolore.scala

stop-all.sh

EOF


cat run_z$1_sep$2.sh
