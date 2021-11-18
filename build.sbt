name := "aurora-dataload"
ThisBuild / organization := "it.luca"
ThisBuild / version := "1.0.0"
ThisBuild / scalaVersion := "2.11.12"
ThisBuild / resolvers += "Cloudera Repo" at "https://repository.cloudera.com/artifactory/cloudera-repos/"

// Compiler option
ThisBuild / javacOptions ++= "-source" :: "1.8" :: "-target" :: "1.8" :: Nil
ThisBuild / scalacOptions ++= "-encoding" :: "UTF-8" ::
  "-target:jvm-1.8" ::
  "-unchecked" ::
  "-deprecation" ::
  "-feature" :: Nil

// Exclude all resources related to extensions to exclude
lazy val extensionsToExclude: Seq[String] = "properties" :: "json" :: "xml" :: "yaml" :: Nil
(Compile / unmanagedResources) := (Compile / unmanagedResources).value
  .filterNot(x => extensionsToExclude.map {
    extension => x.getName.endsWith(s".$extension")
  }.reduce(_ || _))

// Artifact creation
assembly / assemblyOption := (assembly / assemblyOption).value.withIncludeScala(false)
assembly / assemblyJarName := s"${name.value}-${version.value}.jar"
assembly / assemblyMergeStrategy := {
  case PathList("META-INF", _*) => MergeStrategy.discard
  case x =>
    val oldStrategy = (assembly / assemblyMergeStrategy).value
    oldStrategy(x) }

// Dependencies versions
val sparkVersion = "2.4.0-cdh6.3.2"
val scalaTestVersion = "3.2.0"
val scalaMockVersion = "5.1.0"
val scoptVersion = "4.0.0"
val jsqlParserVersion = "4.0"

// Compile dependencies
lazy val sparkCore = "org.apache.spark" %% "spark-core" % sparkVersion % Provided
lazy val sparkSql = "org.apache.spark" %% "spark-sql" % sparkVersion % Provided
lazy val sparkAvro = "org.apache.spark" %% "spark-avro" % sparkVersion % Provided
lazy val scopt = "com.github.scopt" %% "scopt" % scoptVersion
lazy val jsqlParser = "com.github.jsqlparser" % "jsqlparser" % jsqlParserVersion

// Test dependencies
lazy val scalacTic = "org.scalactic" %% "scalactic" % scalaTestVersion
lazy val scalaTest = "org.scalatest" %% "scalatest" % scalaTestVersion % Test
lazy val scalaMock = "org.scalamock" %% "scalamock" % scalaMockVersion % Test

lazy val root = (project in file("."))
  .settings(
    libraryDependencies ++= sparkCore ::
      sparkSql ::
      scopt ::
      scalacTic ::
      scalaTest ::
      scalaMock :: Nil
  ).dependsOn(
  core % "test->test;compile->compile",
  configuration
).aggregate(core, configuration)

lazy val configuration = (project in file("configuration"))
  .settings(
    libraryDependencies ++= sparkSql ::
      sparkAvro ::
      scalacTic ::
      scalaTest ::
      scalaMock :: Nil
  ).dependsOn(
  core % "test->test;compile->compile")

lazy val core = (project in file("core"))
  .settings(
    libraryDependencies ++= sparkSql ::
      jsqlParser ::
      scalacTic ::
      scalaTest ::
      scalaMock :: Nil
  )