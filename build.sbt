val sparkVersion = "2.4.0-cdh6.3.2"
val scoptVersion = "4.0.0"
val lombokVersion = "1.18.10"

lazy val commonSettings = Seq(
  organization := "it.luca",
  name := "aurora-dataload",
  scalaVersion := "2.11.12",
  version := "0.1",

  // Cloudera Repo (for Spark dependencies)
  resolvers +=
    "Cloudera Repo" at "https://repository.cloudera.com/artifactory/cloudera-repos/",

  // Common dependencies
  libraryDependencies ++= "org.apache.spark" %% "spark-core" % sparkVersion % Provided ::
    "org.apache.spark" %% "spark-sql" % sparkVersion % Provided :: Nil
)

lazy val application = (project in file("application"))
  .settings(
    commonSettings,
    libraryDependencies ++= "com.github.scopt" %% "scopt" % scoptVersion :: Nil)
  .dependsOn(core)

lazy val core = (project in file("core"))
  .settings(
    commonSettings,
    libraryDependencies ++= "org.projectlombok" % "lombok" % lombokVersion % Provided :: Nil
  )