addSbtPlugin("com.thesamet"                      % "sbt-protoc"             % "0.99.12")
addSbtPlugin("org.scalastyle"                    %% "scalastyle-sbt-plugin" % "1.0.0")
addSbtPlugin("com.geirsson"                      % "sbt-scalafmt"           % "1.3.0")
addSbtPlugin("com.dwijnand"                      % "sbt-dynver"             % "2.0.0")
addSbtPlugin("com.eed3si9n"                      % "sbt-unidoc"             % "0.4.1")
addSbtPlugin("com.thoughtworks.sbt-api-mappings" % "sbt-api-mappings"       % "2.0.1")
addSbtPlugin("org.foundweekends"                 % "sbt-bintray"            % "0.5.1")
addSbtPlugin("com.timushev.sbt"                  % "sbt-updates"            % "0.3.2")
addSbtPlugin("com.typesafe.sbt"                  % "sbt-ghpages"            % "0.6.2")
addSbtPlugin("com.typesafe.sbt"                  % "sbt-site"               % "1.3.1")
addSbtPlugin("org.scoverage"                     % "sbt-scoverage"          % "1.5.1")
addSbtPlugin("com.typesafe.sbt"                  % "sbt-native-packager"    % "1.2.2")
addSbtPlugin("com.typesafe.sbt"                  % "sbt-multi-jvm"          % "0.4.0")
addSbtPlugin("com.eed3si9n"                      % "sbt-buildinfo"          % "0.7.0")
addSbtPlugin("pl.project13.scala"                % "sbt-jmh"                % "0.2.27")

addSbtPlugin("io.get-coursier" % "sbt-coursier" % "1.0.0")
classpathTypes += "maven-plugin"

libraryDependencies += "com.thesamet.scalapb" %% "compilerplugin" % "0.7.0-rc6"

scalacOptions ++= Seq(
  "-encoding",
  "UTF-8",
  "-feature",
  "-unchecked",
  "-deprecation",
  //"-Xfatal-warnings",
  "-Xlint",
  "-Yno-adapted-args",
  "-Ywarn-dead-code",
  "-Xfuture"
)
