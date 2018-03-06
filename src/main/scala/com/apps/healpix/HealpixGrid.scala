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
package com.apps.healpix

import healpix.essentials.HealpixBase
import healpix.essentials.Pointing

/**
  * Define a Healpix pixelisation scheme given a nside
  *
  * @param nside : (Int)
  *   Resolution of the grid
  * @param hp : (HealpixBase)
  *   Instance of HealpixBase
  * @param ptg : (Pointing)
  *   Instance of Pointing
  *
  */
case class HealpixGrid(
    nside : Int, hp : HealpixBase, ptg : ExtPointing) {

  /**
    * Map (RA/Dec) to Healpix pixel index
    * @param ra : (Double)
    *   RA coordinate
    * @param dec : (Double)
    *   Dec coordinate
    *
    */
  def index(ra : Double, dec : Double) : Long = {
    ptg.theta = ra
    ptg.phi = dec
    hp.ang2pix(ptg)
  }
}
