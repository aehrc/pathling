ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.20"

lazy val root = (project in file("."))
    .settings(
      name := "PathlingScalaApp",
      resolvers ++= Seq(
        Resolver.mavenLocal,
        Resolver.mavenCentral
      ),
      libraryDependencies ++= Seq(
        "au.csiro.pathling" % "library-runtime" % "9.0.0-SNAPSHOT",
        "org.apache.spark" %% "spark-sql" % "3.5.6"
      ),
      run / fork := true,
      run / javaOptions ++= Seq(
        "-ea",
        "-Duser.timezone=UTC",
        "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED",
        "--add-opens=java.base/java.net=ALL-UNNAMED"
      )
    )
