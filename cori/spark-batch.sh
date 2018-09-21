

nargs=$#
if ! [ $nargs -eq 3 ]; then
echo "usage: spark-batch exec N(#nodes) t(mins)"
return
fi

exe=$1

declare -i NODES
NODES=$2

declare -i t
t=$3

if [ $t -gt 30 ]; then
queue=regular
else
queue=debug
fi

echo "running $exe on $NODES nodes for $t mins on queue $queue"

export NODES

#IMG=nersc/spark-2.3.0:v1
#IMG=docker:lgerhardt/test_updated_python:v1
IMG=registry.services.nersc.gov/plaszczy/spark_desc:latest

echo "IMG=$IMG"

if ! [ -d /global/cscratch1/sd/$USER ] then 
echo "creating NodeCache in /global/cscratch1/sd/$USER" 
mkdir /global/cscratch1/sd/$USER
fi

srun -N $NODES -t $t -C haswell -q $queue --image=$IMG --volume="/global/cscratch1/sd/$USER/tmpfiles:/tmp:perNodeCache=size=200G" $exe
