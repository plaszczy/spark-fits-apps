#!/bin/bash
# Copyright 2018 Julien Peloton
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

## SBT Version
SBT_VERSION=2.11.8
SBT_VERSION_SPARK=2.11

## Package version
VERSION=0.1.0

# Package it
sbt ++${SBT_VERSION} assembly

# External JARS
SF=lib/spark-fits_2.11-0.2.0.jar
HP=lib/jhealpix.jar

# Parameters (put your file)
fitsfn="hdfs://134.158.75.222:8020//user/julien.peloton/cat2149.fits"
nside=512

# Run it!
spark-submit \
  --master spark://134.158.75.222:7077 \
  --driver-memory 4g --executor-memory 18g --executor-cores 17 --total-executor-cores 102 \
  --jars ${SF},${HP} \
  --class com.apps.healpix.HealpixProjection \
  target/scala-${SBT_VERSION_SPARK}/HealpixProjection-assembly-${VERSION}.jar \
  $fitsfn $nside

# --executor-cores 17 --total-executor-cores 102 \
