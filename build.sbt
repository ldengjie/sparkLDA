name := "docTag"

version := "1.0"

scalaVersion := "2.10.6"

//libraryDependencies += "org.apache.spark" %% "spark-core" % "1.6.1"
//libraryDependencies += "org.apache.spark" %% "spark-mllib" % "1.6.1"
libraryDependencies += "org.apache.spark" %% "spark-core" % "1.5.0" %"provided"
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "1.5.0"%"provided"
//libraryDependencies += "org.apache.spark" %% "spark-core" % "1.5.0" 
//libraryDependencies += "org.apache.spark" %% "spark-mllib" % "1.5.0"
libraryDependencies += "org.apache.lucene" % "lucene-core" % "4.0.0"
libraryDependencies += "org.wltea.ik-analyzer" % "ik-analyzer" % "3.2.8"
libraryDependencies += "org.apache.poi" % "poi-ooxml" % "3.14"
libraryDependencies += "org.apache.poi" % "poi-scratchpad" % "3.14"
libraryDependencies += "org.apache.pdfbox" % "pdfbox" % "2.0.2"
libraryDependencies += "org.elasticsearch" %% "elasticsearch-spark" %"2.3.4"
//libraryDependencies += "com.databricks" % "spark-csv_2.10" % "1.2.0"

packAutoSettings

assemblyMergeStrategy in assembly := {
    case PathList("javax", "servlet", xs @ _*) => MergeStrategy.last
    case PathList("javax", "activation", xs @ _*) => MergeStrategy.last
    case PathList("org", "apache", xs @ _*) => MergeStrategy.last
    case PathList("com", "google", xs @ _*) => MergeStrategy.last
    case PathList("com", "esotericsoftware", xs @ _*) => MergeStrategy.last
    case PathList("com", "codahale", xs @ _*) => MergeStrategy.last
    case PathList("com", "yammer", xs @ _*) => MergeStrategy.last
    case "about.html" => MergeStrategy.rename
    case "META-INF/ECLIPSEF.RSA" => MergeStrategy.last
    case "META-INF/mailcap" => MergeStrategy.last
    case "META-INF/mimetypes.default" => MergeStrategy.last
    case "plugin.properties" => MergeStrategy.last
    case "log4j.properties" => MergeStrategy.last
    case x =>
        val oldStrategy = (assemblyMergeStrategy in assembly).value
        oldStrategy(x)
}
