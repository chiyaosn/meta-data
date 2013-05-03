organization := "com.servicenow.syseng"

name := "meta-data"

version := "1.0-SNAPSHOT"

resolvers ++= Seq( "snc-lab" at "http://10.196.32.21:8081/nexus/content/groups/public/" )

libraryDependencies ++= Seq(
  "joda-time" % "joda-time" % "2.1",
  "org.joda" % "joda-convert" % "1.2",
  "nl.grons" %% "metrics-scala" % "2.2.0",
  "com.yammer.metrics" % "metrics-graphite" % "2.2.0",
  "io.spray" % "spray-client" % "1.0-M7",
  "com.fasterxml.jackson.core" % "jackson-core" % "2.1.4",
  "com.fasterxml.jackson.core" % "jackson-databind" % "2.1.4",
  "com.fasterxml.jackson.module" % "jackson-module-scala_2.9.2" % "2.1.3",
  "com.servicenow.syseng" % "data-model" % "1.0-SNAPSHOT"  
  // TODO : add hadoop libraries
)

// disable using the Scala version in output paths and artifacts
crossPaths := false

//publishTo := Some(Resolver.file("file",  new File( "/Users/chi.yao/.ivy2/local/" )) )
publishTo := Some("snc-lab-snapshot" at "http://10.196.32.21:8081/nexus/content/repositories/snapshots/" )

publishMavenStyle := true

credentials += Credentials(Path.userHome / ".ivy2" / ".credentials")
//credentials += Credentials("Sonatype Nexus Repository Manager", "10.196.32.21", "<me>","<my_password>")
