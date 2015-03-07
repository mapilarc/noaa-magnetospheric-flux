package org.pilar.noaa

class FluxChannel(CODE: String, YR: Int, MO: Int, DA: Int, HHMM: Int, P1: Double, P2: Double, P3: Double, P4: Double, P5: Double, E1: Double, E2: Double, E3: Double, E4: Double, E5: Double) {
  var code : String = CODE
  var yr: Int = YR
  var mo: Int = MO
  var da: Int = DA
  var hhmm: Int = HHMM
  var p1: Double = P1
  var p2: Double = P2
  var p3: Double = P3
  var p4: Double = P4
  var p5: Double = P5
  var e1: Double = E1
  var e2: Double = E2
  var e3: Double = E3
  var e4: Double = E4
  var e5: Double = E5

  def toMessage(): String = "%s;%d;%d;%d;%d;%d;%d;%d;%d;%d;%d;%d;%d;%d;%d".format(code, yr, mo, da, hhmm, p1.toInt, p2.toInt, p3.toInt, p4.toInt, p5.toInt, e1.toInt, e2.toInt, e3.toInt, e4.toInt, e5.toInt)
}