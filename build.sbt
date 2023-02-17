// The simplest possible sbt build file is just one line:

scalaVersion := "2.13.8"

name := "spark-example"
organization := "org.example"
version := "1.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % "3.3.2"
)

javaOptions ++= Seq(
  "base/java.lang",
  "base/java.lang.invoke",
  "base/java.lang.reflect",
  "base/java.io",
  "base/java.net",
  "base/java.nio",
  "base/java.util",
  "base/java.util.concurrent",
  "base/java.util.concurrent.atomic",
  "base/sun.nio.ch",
  "base/sun.nio.cs",
  "base/sun.security.action",
  "base/sun.util.calendar",
  "security.jgss/sun.security.krb5"
).map("--add-opens=java." + _ + "=ALL-UNNAMED")

fork in run := true
