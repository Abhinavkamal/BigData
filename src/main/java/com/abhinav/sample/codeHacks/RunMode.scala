
object RunMode extends Enumeration{
  type RunMode = Value
  val PRODUCTION, UNIT_TEST = Value
}

val runMode  = RunMode.PRODUCTION

 val configPath = runMode match {
  case RunMode.PRODUCTION => "Prodpath"
  case RunMode.UNIT_TEST => "TestPath"
}