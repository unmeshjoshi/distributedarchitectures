import com.typesafe.sbt.packager.archetypes.JavaAppPackaging
import sbt.Keys.{name, packageBin, version}
import sbt.{AutoPlugin, Plugins, Setting}
object DeployApp extends AutoPlugin {
  import com.typesafe.sbt.packager.SettingsHelper
  import com.typesafe.sbt.packager.universal.UniversalPlugin
  import UniversalPlugin.autoImport.{Universal, UniversalDocs}
  import sbtbuildinfo.BuildInfoPlugin
  import BuildInfoPlugin.autoImport._

  override def requires: Plugins = UniversalPlugin && JavaAppPackaging && BuildInfoPlugin

  override def projectSettings: Seq[Setting[_]] =
    SettingsHelper.makeDeploymentSettings(Universal, packageBin in Universal, "zip") ++
      SettingsHelper.makeDeploymentSettings(UniversalDocs, packageBin in UniversalDocs, "zip") ++ Seq(
      buildInfoKeys := Seq[BuildInfoKey](name, version),
      buildInfoPackage := "micro.services"
    )
}
