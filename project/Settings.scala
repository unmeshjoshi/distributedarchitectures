import sbt.Keys._
import sbt._

//noinspection TypeAnnotation
// Defines the global build settings so they don't need to be edited everywhere
object Settings {

  val buildSettings = Seq(
    organization := "org.http2example",
    organizationName := "example",
    organizationHomepage := Some(url("http://www.example.org")),
    version := Dependencies.Version,
    scalaVersion := Libs.ScalaVersion,
    crossPaths := true,
    parallelExecution in Test := false,
    fork := true
  )

  lazy val defaultSettings = buildSettings ++ Seq(
    // compile options ScalaUnidoc, unidoc
    scalacOptions ++= Seq("-encoding", "UTF-8", "-feature", "-deprecation", "-unchecked"),
    javacOptions in(Compile, compile) ++= Seq("-source", "8", "-target", "8", "-Xlint:unchecked", "-Xlint:deprecation"),
    javaOptions in(Test, run) ++= Seq("-Djava.net.preferIPv4Stack=true") // For location service use
  )
}
