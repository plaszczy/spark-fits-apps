/*
 * Copyright 2018 Julien Peloton
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
import Dependencies._

lazy val root = (project in file(".")).
 settings(
   inThisBuild(List(
     version      := "0.1.0",
     mainClass in Compile := Some("com.apps.healpix.HealpixProjection")
   )),
   // Name of the application
   name := "HealpixProjection",
   // Excluding Scala library JARs that are included in the binary Scala distribution
   assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false),
   // Shading to avoid conflicts with pre-installed library
   // Uncomment if you have such conflicts, and adapt the name to the desired library.
   // assemblyShadeRules in assembly := Seq(ShadeRule.rename("nom.**" -> "new_nom.@1").inAll),
   // Put dependencies of the library
   libraryDependencies ++= Seq(
     "org.apache.spark" %% "spark-core" % "2.1.0" % "provided",
     "org.apache.spark" %% "spark-sql" % "2.1.0" % "provided",
     scalaTest % Test
   )
 )
