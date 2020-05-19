#!/bin/bash
# Copyright 2018 Julien Peloton
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

nargs=$#
echo "nargs=$nargs"
if [ $nargs -eq 0 ] ; then
echo "missing shell [pyspark/spark-shell opts (jup)]"
exit
fi

n_executors=9
if [ $nargs -gt 1 ] ; then
    n_executors=$2
fi
echo "runinng on  ${n_executors} executors"

#machine specific
source sparkopts.sh ${n_executors}

#add sparkfits
MYJARS="$HOME/spark-fits-apps/lib/spark-fits_2.11-0.6.0.jar,$HOME/spark-fits-apps/lib/jhealpix.jar"

# Run it!
cmd="$1 $SPARKOPTS --jars $MYJARS"

export PYSPARK_DRIVER_PYTHON=ipython

#jup
if [ $nargs -eq 3 ]; then 
export PYSPARK_DRIVER_PYTHON=
cmd="PYSPARK_DRIVER_PYTHON_OPTS='/opt/anaconda/bin/jupyter-notebook --no-browser --port=24501' $cmd"
fi
echo "running >>>>>>>>>>>>>>>>"
echo $cmd
echo "<<<<<<<<<<<<<<<<<<<<<<<<"

eval $cmd
