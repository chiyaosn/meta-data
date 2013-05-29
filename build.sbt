import AssemblyKeys._
import sbtassembly.Plugin._
import sbt._
import Keys._

seq(assemblySettings: _*)

organization := "com.servicenow.bigdata"

name := "meta-data"

version := "1.0-SNAPSHOT"

resolvers ++= Seq( "snc-lab" at "http://10.196.32.21:8081/nexus/content/groups/public/" )

mergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) =>
    (xs map {_.toLowerCase}) match {
      case ("manifest.mf" :: Nil) => MergeStrategy.discard
      case _ => MergeStrategy.first
    }
  case _ => MergeStrategy.first
}

libraryDependencies ++= Seq(
  "joda-time" % "joda-time" % "2.1",
  "org.joda" % "joda-convert" % "1.2",
  "nl.grons" %% "metrics-scala" % "2.2.0",
  "com.yammer.metrics" % "metrics-graphite" % "2.2.0",
  "io.spray" % "spray-client" % "1.0-M7",
  "com.fasterxml.jackson.core" % "jackson-core" % "2.1.4",
  "com.fasterxml.jackson.core" % "jackson-databind" % "2.1.4",
  "com.fasterxml.jackson.module" % "jackson-module-scala_2.9.2" % "2.1.3"
  // TODO : add hadoop libraries
)

artifact in (Compile, assembly) ~= { art =>
  art.copy(`classifier` = Some("assembly"))
}

addArtifact(artifact in (Compile, assembly), assembly)

// disable using the Scala version in output paths and artifacts
crossPaths := false

//publishTo := Some(Resolver.file("file",  new File( Path.userHome+"/.ivy2/local/" )) )
publishTo := Some("snc-lab-snapshot" at "http://10.196.32.21:8081/nexus/content/repositories/snapshots/" )

publishMavenStyle := true

credentials += Credentials(Path.userHome / ".ivy2" / ".credentials")
//credentials += Credentials("Sonatype Nexus Repository Manager", "10.196.32.21", "<me>","<my_password>")
