val sparkVersion = "2.4.0-cdh6.3.2"
val scalaTestVersion = "3.2.0"
val scoptVersion = "4.0.0"
val lombokVersion = "1.18.10"
val jsqlParserVersion = "4.0"

lazy val commonSettings = Seq(
  organization := "it.luca",
  name := "aurora-dataload",
  scalaVersion := "2.11.12",
  version := "0.1",

  // Java compiler options
  javacOptions ++= "-source" :: "1.8" ::
    "-target" :: "1.8" :: Nil,

  // Scala options
  scalacOptions ++= "-encoding" :: "UTF-8" ::
    "-target:jvm-1.8" ::
    "-feature" :: "-language:implicitConversions" :: Nil,

  // Cloudera Repo (for Spark dependencies)
  resolvers +=
    "Cloudera Repo" at "https://repository.cloudera.com/artifactory/cloudera-repos/",

  // Common dependencies
  libraryDependencies ++= "org.apache.spark" %% "spark-core" % sparkVersion ::
    "org.apache.spark" %% "spark-sql" % sparkVersion ::
    "org.scalactic" %% "scalactic" % scalaTestVersion ::
    "org.scalatest" %% "scalatest" % scalaTestVersion % Test :: Nil
)

lazy val root = (project in file("."))
  .aggregate(application, core)

lazy val application = (project in file("application"))
  .settings(
    commonSettings,
    libraryDependencies ++= "com.github.scopt" %% "scopt" % scoptVersion :: Nil)
  .dependsOn(core)

lazy val core = (project in file("core"))
  .settings(
    commonSettings,
    libraryDependencies ++= "org.projectlombok" % "lombok" % lombokVersion % Provided ::
      "com.github.jsqlparser" % "jsqlparser" % jsqlParserVersion :: Nil
  )