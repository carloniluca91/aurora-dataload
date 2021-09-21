// Dependencies versions
val sparkVersion = "2.4.0-cdh6.3.2"
val scalaTestVersion = "3.2.0"
val scalaMockVersion = "5.1.0"
val scoptVersion = "4.0.0"
val lombokVersion = "1.18.10"
val jsqlParserVersion = "4.0"
val jacksonVersion = "2.9.9"

// Compile dependencies
lazy val sparkCore = "org.apache.spark" %% "spark-core" % sparkVersion % Provided
lazy val sparkSql = "org.apache.spark" %% "spark-sql" % sparkVersion % Provided
lazy val sparkAvro = "org.apache.spark" %% "spark-avro" % sparkVersion
lazy val scopt = "com.github.scopt" %% "scopt" % scoptVersion
lazy val jacksonYaml = "com.fasterxml.jackson.dataformat" % "jackson-dataformat-yaml" % jacksonVersion
lazy val lombok = "org.projectlombok" % "lombok" % lombokVersion % Provided
lazy val jsqlParser = "com.github.jsqlparser" % "jsqlparser" % jsqlParserVersion

// Test dependencies
lazy val scalacTic = "org.scalactic" %% "scalactic" % scalaTestVersion
lazy val scalaTest = "org.scalatest" %% "scalatest" % scalaTestVersion % Test
lazy val scalaMock = "org.scalamock" %% "scalamock" % scalaMockVersion % Test

// Common settings
lazy val commonSettings = Seq(
  organization := "it.luca",
  scalaVersion := "2.11.12",
  version := "0.1",

  // Java compiler options
  javacOptions ++= "-source" :: "1.8" ::
    "-target" :: "1.8" :: Nil,

  // Scala options
  scalacOptions ++= "-encoding" :: "UTF-8" ::
    "-target:jvm-1.8" ::
    "-feature" :: "-language:implicitConversions" :: Nil,

  // Compile Java sources first
  compileOrder := CompileOrder.JavaThenScala,

  // Cloudera Repo (for Spark dependencies)
  resolvers +=
    "Cloudera Repo" at "https://repository.cloudera.com/artifactory/cloudera-repos/",
)

lazy val extensionsToExclude: Seq[String] = "properties" :: "xml" :: "yaml" :: Nil
lazy val dataload = (project in file("."))
  .settings(name := "aurora-dataload")
  .aggregate(application, core, configuration)

lazy val application = (project in file("application"))
  .settings(
    commonSettings,
    libraryDependencies ++= sparkCore ::
      sparkSql ::
      scopt ::
      scalacTic ::
      scalaTest ::
      scalaMock :: Nil,

    // Exclude all resources related to extensions to exclude
    (unmanagedResources in Compile) := (unmanagedResources in Compile).value
      .filterNot(x => extensionsToExclude.map {
        extension => x.getName.endsWith(s".$extension")
      }.reduce(_ || _)),

    assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false),
    assemblyJarName in assembly := s"aurora_dataload.jar",
    assemblyMergeStrategy in assembly := {
      case PathList("META-INF", _*) => MergeStrategy.discard
      case x =>
        val oldStrategy = (assemblyMergeStrategy in assembly).value
        oldStrategy(x) }
  )
  .dependsOn(core, configuration)

lazy val core = (project in file("core"))
  .settings(
    commonSettings,
    libraryDependencies ++= sparkSql ::
      jacksonYaml ::
      lombok ::
      jsqlParser ::
      scalacTic ::
      scalaTest ::
      scalaMock :: Nil
  )

lazy val configuration = (project in file("configuration"))
  .settings(
    name := "configuration",
    commonSettings,
    libraryDependencies ++= sparkSql ::
      sparkAvro ::
      jacksonYaml ::
      lombok ::
      scalacTic ::
      scalaTest ::
      scalaMock :: Nil
  )