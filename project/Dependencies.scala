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
    Libs.`scalaTest` % Test,
    Libs.`mockito` % Test
  )
}
