import Settings._

concurrentRestrictions in Global += Tags.limit(Tags.Test, 1)

val `distributedarchitectures` = project
  .in(file("."))
  .enablePlugins(DeployApp, DockerPlugin)
  .settings(defaultSettings: _*)
  .settings(
     libraryDependencies ++= Dependencies.Dist
)


