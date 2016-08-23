package com.sugon.spark.LDA

import java.io.{PrintWriter, File, StringReader}
import java.text.BreakIterator
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute
import org.wltea.analyzer.lucene.IKAnalyzer
import scala.collection.mutable
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.mllib.clustering._
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD
import scala.io.Source
import org.elasticsearch.spark._
//.docx .xlsx
import org.apache.poi.POIXMLDocument
import org.apache.poi.xwpf.extractor.XWPFWordExtractor
import org.apache.poi.xssf.extractor.XSSFExcelExtractor
import org.apache.poi.xslf.extractor.XSLFPowerPointExtractor
//.pdf
import org.apache.pdfbox.pdmodel.PDDocument
import org.apache.pdfbox.text.PDFTextStripper

object LDATest {

  //样例类,自动具备 apply,unapply,toString,equals,hashCode,copy 方法,可print。用普通类的主构造器也可以实现，不过字段前要加val/var，否则变量没有被使用，不会升为字段
  private case class Params(
    input: String = "/root/workSpark/sparkLDA/data/shuiWu/txt/",
    //input: String = "/root/workSpark/sparkLDA/data/test/",
    k: Int = 20,                         
    //maxIterations: Int = 200,             
    docConcentration: Double = -1,      
    topicConcentration: Double = -1,    
    vocabSize: Int = 30000,      
    algorithm: String = "em",          
    checkpointDir: Option[String] = None,
    checkpointInterval: Int = 50 ,     
    esoutput: String = "ldj/test",
    esnodes: String = "10.0.51.18",
    esport: String = "9200"
  )

  def main(args: Array[String]) {
    run(Params())
  }

  private def run(params: Params) {

    val conf = new SparkConf().setAppName(s"LDAExample with $params").setMaster("local[2]")
    conf.set("es.index.auto.create", "true")
    conf.set("pushdown","true")
    conf.set("es.nodes" , params.esnodes)
    conf.set("es.port", params.esport)

    val sc = new SparkContext(conf)
    Logger.getRootLogger.setLevel(Level.WARN)
    val preprocessStart = System.nanoTime()
    val (corpus, vocabArray, actualNumTokens, pathRdd,titleRdd,contentRdd) = preprocess(sc, params.input, params.vocabSize)
    corpus.cache()
    val actualCorpusSize = corpus.count()
    val actualVocabSize = vocabArray.size
    val preprocessElapsed = (System.nanoTime() - preprocessStart) / 1e9
    println()
    println(s"Corpus summary:")
    println(s"\t Training set size: $actualCorpusSize documents")
    println(s"\t Vocabulary size: $actualVocabSize terms")
    println(s"\t Training set size: $actualNumTokens tokens")
    println(s"\t Preprocessing time: $preprocessElapsed sec")
    println()

    //配置LDA
    val lda = new LDA()
    val optimizer = params.algorithm.toLowerCase match {
      case "em" => new EMLDAOptimizer
      case "online" => new OnlineLDAOptimizer().setMiniBatchFraction(0.05 + 1.0 / actualCorpusSize)
      case _ => throw new IllegalArgumentException(
        s"Only em, online are supported but got ${params.algorithm}.")
    }

    lda.setOptimizer(optimizer)
      .setK(params.k)
      //.setMaxIterations(params.maxIterations)
        .setDocConcentration(params.docConcentration)
        .setTopicConcentration(params.topicConcentration)
        .setCheckpointInterval(params.checkpointInterval)

        if (params.checkpointDir.nonEmpty) {
          sc.setCheckpointDir(params.checkpointDir.get)
        }

        val startTime = System.nanoTime()
        val ldaModel = lda.run(corpus)
        val elapsed = (System.nanoTime() - startTime) / 1e9

        println(s"Finished training LDA model.  Summary:")
        println(s"\t Training time: $elapsed sec")

        //提取结果
        val topicIndices = ldaModel.describeTopics(maxTermsPerTopic = 10)
        val topics = topicIndices.map { case (terms, termWeights) =>
          terms.zip(termWeights).map { case (term, weight) => (vocabArray(term.toInt), weight) }
        }


        //topic的关键词
        import scala.collection.mutable.Map
        var topicMap = Map[Int, String]()
        topics.zipWithIndex.foreach { case (topic, i) =>
          val topicMapKey = i
          var topicMapValue = ""
          topic.foreach { case (term, weight) =>
            topicMapValue += term + ","
          }
          topicMap += (topicMapKey->topicMapValue.substring(0,topicMapValue.length-1))
        }
        topicMap.foreach(println)

        //保存数据到es
        if (ldaModel.isInstanceOf[DistributedLDAModel]) {
          val distLDAModel = ldaModel.asInstanceOf[DistributedLDAModel]
          val avgLogLikelihood = distLDAModel.logLikelihood / actualCorpusSize.toDouble
          println(s"\t Training data average log likelihood: $avgLogLikelihood")
          val topicToFile= distLDAModel.topicDistributions.flatMap{
            case(id,value) => List(id-> value.toArray.zipWithIndex.max)
          }
          .join(pathRdd)
          .join(titleRdd)
          .join(contentRdd)
          .map{
            case (id,((((rate,topicId),path),title),content)) =>{
              Map("document" -> path, "title" -> title, "keywords" -> topicMap(topicId), "topic" -> topicId.toString, "rate" -> rate.toString, "content" -> content)
            }
          }
          //topicToFile.foreach(println)

            topicToFile.saveToEs(params.esoutput)
        }
        sc.stop()
  }

  //准备语料库id+document稀疏矩阵（分词后去掉了停词），词库数组，词数量
  private def preprocess(
    sc: SparkContext,
    paths: String,
    vocabSize: Int)= {

      //读取待分析文档，10个slices
      val originalFileRDD = sc.wholeTextFiles(paths+"/*",8)

      val wholeTextRDD=originalFileRDD.map{
        case (file,stream)=>
          val suffixRegex="""file:(.*)\.(\w+)$""".r
          val (path,suffix)=suffixRegex.findAllIn(file).matchData.map{m=>(m.group(1),m.group(2))}.toMap.head
          val filePath=path+"."+suffix
          val content =  suffix match {
            case "txt"  => {
              var txtContent:String=""
              try{
                txtContent=Source.fromFile(filePath,"GBK").mkString
              }catch{
                case _ :Throwable => {
                  txtContent=Source.fromFile(filePath,"UTF-8").mkString
                }
              }
              txtContent
            }
            case "docx" => new XWPFWordExtractor(POIXMLDocument.openPackage(filePath)).getText
            case "pptx" => new XSLFPowerPointExtractor(POIXMLDocument.openPackage(filePath)).getText();
            case "xlsx" => new XSSFExcelExtractor(POIXMLDocument.openPackage(filePath)).getText()
            //case "pdf"  => new PDFTextStripper().getText(PDDocument.load(new File(filePath))) 
            case "pdf"  =>{
              val doc=PDDocument.load(new File(filePath))
              val content=new PDFTextStripper().getText(doc)
              doc.close()
              content
            }
            case _   => 
          }
          (file,content)
      }
      //.map{
      //case (file,content)=>{
      //println(file,content)
      //(file,content)
      //}
      //}
      .filter(_._2.toString.nonEmpty)
      .zipWithIndex
      .map{
        case ((file,content),id)=>
            (
              id -> 
              (
                file.replaceAll("^file:","")
                , content.toString.substring(0,content.toString.indexOf("\n"))
                , content.toString.replaceAll("\n"," ").replaceAll("\r"," ")
              )
            )
      }
      //.map{
      //case ((id,file),content)=>{
      //println(id, file)
      //((id,file),content)
      //}
      //}

      wholeTextRDD.cache()
      
      val pathRDD=wholeTextRDD.map{
        case (id,(path,title,content))=>{
          (id,path)
        }
      }
      val contentRDD=wholeTextRDD.map{
        case (id,(path,title,content))=>{
          (id,content)
        }
      }
      val titleRDD=wholeTextRDD.map{
        case (id,(path,title,content))=>{
          (id,title)
        }
      }


      val minWordLength = 2
      val pretextRDD = contentRDD
      //转换成流，作为tokenStream输入参数
        .map{ case (id,line) =>(id, new StringReader(line)) }
        //分词
        .map{ case (id,line) =>(id, new IKAnalyzer(true).tokenStream("",line)) }
        .map{ case (id,y) =>
          val term: CharTermAttribute = y.getAttribute(classOf[CharTermAttribute])
          var newline = new mutable.ArrayBuffer[String]()
          while (y.incrementToken){
            if (term.toString.length >= minWordLength) newline += term.toString
          }
          //println(id,newline)
         (id, newline)
        }
        pretextRDD.cache()

        //文档中的词频
        val wordCounts: RDD[(String, Long)] = pretextRDD
          .flatMap { case (_, tokens) => tokens.map(_ -> 1L) }
          .reduceByKey(_ + _)

          wordCounts.cache()
          //println("")
          //println("vocabulary:")
          //println("============")
          //wordCounts.sortBy(_._2, ascending = false).foreach(println)
          //println("============")

          //取前 vocabSize 个词，然后要求词频>5，生成词库列表[word，Index]，词的数量
          val fullVocabSize = wordCounts.count()
          val (vocab: Map[String, Int], selectedTokenCount: Long) = {
            val tmpSortedWCpre: Array[(String, Long)] = if (vocabSize == -1 || fullVocabSize <= vocabSize) {
              wordCounts.collect().sortBy(-_._2)
            } else {
              wordCounts.sortBy(_._2, ascending = false).take(vocabSize)
            }
            val tmpSortedWC = tmpSortedWCpre.filter(_._2>0)
            (tmpSortedWC.map(_._1).zipWithIndex.toMap, tmpSortedWC.map(_._2).sum)
          }

          //文档-词频 矩阵
          val documents = pretextRDD.map { case (id, tokens) =>
            val wc = new mutable.HashMap[Int, Int]()
            tokens.foreach { term =>
              if (vocab.contains(term)) {
                val termIndex = vocab(term)
                wc(termIndex) = wc.getOrElse(termIndex, 0) + 1
              }
            }
            val indices = wc.keys.toArray.sorted
            val values = indices.map(i => wc(i).toDouble)
            val sb = Vectors.sparse(vocab.size, indices, values)
            (id, sb)
          }

          //词库数组
          val vocabArray = new Array[String](vocab.size)
          vocab.foreach { case (term, i) => vocabArray(i) = term }
          (documents, vocabArray, selectedTokenCount, pathRDD,titleRDD,contentRDD)
          //(documents, vocabArray, selectedTokenCount)
    }
}
