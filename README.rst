================
Spark Fits Apps
================

The package
================

This repository provides a set of applications using spark-fits.
You can find:

* Projection of galaxies from a catalog (FITS) into a Healpix map.

Requirements
================

Most of the applications requires `Apache Spark <http://spark.apache.org/>`_ 2.0+ (not tested for earlier version),
`spark-fits <https://github.com/JulienPeloton/spark-fits>`_ 0.2.0+,
and `Java Healpix <http://healpix.sourceforge.net/>`_ (latest).
Spark-fits library has been tested with Scala 2.10.6 and 2.11.X.

Since there is no maven coordinates yet for spark-fits and healpix, you need to provide the JAR,
and assemble the project (recipe to assemble provided in the ``run*.sh`` scripts).
Note that we provide the JAR for healpix under the folder ``lib/`` at the root
of the project (compiled with Scala 2.11.8) if you do not want to build yourself the libraries.
However you need to compile yourself spark-fits and add the JAR under the folder ``lib``
(or provide the full path).
