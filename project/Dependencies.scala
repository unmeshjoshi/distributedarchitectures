import sbt._

object Dependencies {

  val Version = "0.1-SNAPSHOT"
  val Dist = Seq(
    Libs.`zookeeper`,
    Libs.`zkclient`,
    Libs.`yammer`,
    Libs.`scalaLogging`,
    Libs.`jacksonDatabind`,
    Libs.`jacksonJaxrsJsonProvider`,
    Libs.`jacksonJDK8Datatypes`,
    Libs.`jacksonDataformatCsv`,
    Libs.`jacksonModuleScala`,
    Libs.`scalaCollectionCompat`,
    Libs.`googleGuava`,
    Libs.`jamm`,
    Libs.`pcj`,
    Libs.`commons-codec`,
    Libs.`disruptor`,
    Libs.`akka-stream-typed`,
    Libs.`scalaTest` % Test,
    Libs.`mockito` % Test,
    Libs.`junit`  % Test,
    Libs.`jmh-core` % Test,
    Libs.`jmh_bench` % Test,
    Libs.`jmh_generator_ann` % Test,
    Libs.`jmh_generator_bytecode` % Test
  )
}
