lazy val commonSettings = Seq(
	name := "gcamProject",
	version := "1.0",
	scalaVersion := "2.12.8"
)

lazy val commonDependencies = Seq(
	"org.apache.spark" 		%% "spark-core" % "2.4.0",
	"org.apache.spark" 		%% "spark-sql"  % "2.4.0",
	"org.apache.hadoop" % "hadoop-client" % "2.8.5",
	"com.github.davidmoten" % "geo" % "0.7.1"
)

lazy val root = (project in file("."))
	.settings(
		commonSettings,
		libraryDependencies ++= commonDependencies)
	.dependsOn(geo)


lazy val geo = RootProject(uri("https://github.com/davidmoten/geo.git")) 