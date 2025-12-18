import sbtassembly.AssemblyPlugin.autoImport._
import scala.collection.immutable.Seq

ThisBuild / version := "1.0.0"
ThisBuild / scalaVersion := "2.12.20"

lazy val root = (project in file("."))
  .settings(
    name := "untitled3",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % "3.5.3",
      "org.apache.spark" %% "spark-sql" % "3.5.3",
      "org.apache.hadoop" % "hadoop-aws" % "3.3.4",
      "org.apache.hadoop" % "hadoop-common" % "3.3.4",
      "com.amazonaws" % "aws-java-sdk-bundle" % "1.12.262",
      "org.apache.logging.log4j" % "log4j-api" % "2.20.0",
      "org.apache.logging.log4j" % "log4j-core" % "2.20.0",
      "org.apache.logging.log4j" % "log4j-slf4j2-impl" % "2.20.0"
    ),
    assemblyMergeStrategy := {
      case PathList("META-INF", _ @ _*) => MergeStrategy.discard
      case _ => MergeStrategy.first
    },
  )
