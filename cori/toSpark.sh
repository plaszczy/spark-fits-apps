

nargs=$#
if ! [ $nargs -eq 2 ]; then
echo "usage: toSpark N(#nodes) t(mins)"
return
fi
declare -i NODES
NODES=$1

declare -i t
t=$2

if [ $t -gt 30 ]; then
queue=interactive
else
queue=debug
fi

echo "running on $NODES nodes for $t mins on queue $queue"

export NODES

#IMG=nersc/spark-2.3.0:v1
#IMG=docker:lgerhardt/test_updated_python:v1
IMG=registry.services.nersc.gov/plaszczy/spark_desc:latest

echo "IMG=$IMG"

if ! [ -d /global/cscratch1/sd/$USER ] then 
echo "creating NodeCache in /global/cscratch1/sd/$USER" 
mkdir /global/cscratch1/sd/$USER
fi

salloc -N $NODES -t $t -C haswell -q $queue --image=$IMG --volume="/global/cscratch1/sd/$USER/tmpfiles:/tmp:perNodeCache=size=200G"
