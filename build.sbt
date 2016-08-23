name := "docTag"

version := "1.0"

scalaVersion := "2.10.6"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.6.1"
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "1.6.1"
libraryDependencies += "org.apache.lucene" % "lucene-core" % "4.0.0"
libraryDependencies += "org.wltea.ik-analyzer" % "ik-analyzer" % "3.2.8"
libraryDependencies += "org.apache.poi" % "poi-ooxml" % "3.14"
libraryDependencies += "org.apache.pdfbox" % "pdfbox" % "2.0.2"
//libraryDependencies += "org.apache.pdfbox" % "pdfbox" % "1.8.12"
//libraryDependencies += "org.apache.poi" % "poi" % "3.14"
//libraryDependencies += "org.elasticsearch" % "elasticsearch-hadoop" %"2.3.2"
//libraryDependencies += "org.elasticsearch" % "elasticsearch-hadoop" %"2.2.0" 
libraryDependencies += "org.elasticsearch" %% "elasticsearch-spark" %"2.3.4"
