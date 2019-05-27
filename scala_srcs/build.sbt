name := "kernel-lib"

scalaVersion := "2.11.12"

// https://mvnrepository.com/artifact/org.apache.spark/spark-sql
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.2"

// https://mvnrepository.com/artifact/org.apache.spark/spark-core
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.2"

// https://mvnrepository.com/artifact/org.scalanlp/breeze-natives
libraryDependencies += "org.scalanlp" %% "breeze-natives" % "1.0-RC2"

// https://mvnrepository.com/artifact/org.scalanlp/breeze
libraryDependencies += "org.scalanlp" %% "breeze" % "1.0-RC2"

// up to understanding how to set up propper version...
// https://mvnrepository.com/artifact/com.github.fommil.netlib/all
// libraryDependencies += "com.github.fommil.netlib" % "all" % "1.1.2" pomOnly()
