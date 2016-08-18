package com.sugon.spark.LDA

import java.io.{PrintWriter, File, StringReader}
import java.text.BreakIterator
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute
import org.apache.spark.examples.mllib.AbstractParams
import org.wltea.analyzer.lucene.IKAnalyzer
import scala.collection.mutable
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.mllib.clustering._
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD
import scala.io.Source
import org.apache.poi.POIXMLDocument;
import org.apache.poi.POITextExtractor; 
import org.apache.poi.xwpf.extractor.XWPFWordExtractor; 
import org.apache.poi.xwpf.usermodel.XWPFDocument; 

object LDATest {


  //样例类,自动具备 apply,unapply,toString,equals,hashCode,copy 方法
  private case class Params(
    input: String = "/root/workSpark/sparkLDA/data/shuiWu/txt/",
    //input: String = "/root/workSpark/sparkLDA/data/test/",
    k: Int = 20,                         
    maxIterations: Int = 200,             
    docConcentration: Double = -1,      
    topicConcentration: Double = -1,    
    vocabSize: Int = 30000,      
    stopwordFile: String = "/root/workSpark/sparkLDA/src/stopword.dic",        
    algorithm: String = "em",          
    checkpointDir: Option[String] = None,
    checkpointInterval: Int = 50
    //,     
    //esoutput: String = "model/ldaresult6",
    //esnodes: String = "192.168.59.128",
    //esport: String = "9200"
  ) extends AbstractParams[Params]

  def main(args: Array[String]) {
    run(Params())
  }

  private def run(params: Params) {

    val conf = new SparkConf().setAppName(s"LDAExample with $params").setMaster("local[2]")
    //conf.set("es.index.auto.create", "true")
    //conf.set("pushdown","true")
    //conf.set("es.nodes" , params.esnodes)
    //conf.set("es.port", params.esport)

    val sc = new SparkContext(conf)
    Logger.getRootLogger.setLevel(Level.WARN)
    val preprocessStart = System.nanoTime()
    val (corpus, vocabArray, actualNumTokens, filePaths) = preprocess(sc, params.input, params.vocabSize, params.stopwordFile)
    //val (corpus, vocabArray, actualNumTokens) = preprocess(sc, params.input, params.vocabSize, params.stopwordFile)
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
      .setMaxIterations(params.maxIterations)
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
        val topicIndices = ldaModel.describeTopics(maxTermsPerTopic = 6)
        val topics = topicIndices.map { case (terms, termWeights) =>
          terms.zip(termWeights).map { case (term, weight) => (vocabArray(term.toInt), weight) }
        }


        //topic得关键词
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

        //topic->文件位置
        if (ldaModel.isInstanceOf[DistributedLDAModel]) {
          val distLDAModel = ldaModel.asInstanceOf[DistributedLDAModel]
          val avgLogLikelihood = distLDAModel.logLikelihood / actualCorpusSize.toDouble
          println(s"\t Training data average log likelihood: $avgLogLikelihood")
          val topicToFile= distLDAModel.topicDistributions.flatMap{
            case(key,value) => List(key -> value.toArray.zipWithIndex.max._2)
          }.join(filePaths).values
          topicToFile.foreach(println)
        }
        sc.stop()
  }

  //准备语料库id+document稀疏矩阵（分词后去掉了停词），词库数组，词数量
  private def preprocess(
    sc: SparkContext,
    paths: String,
    vocabSize: Int,
    stopwordFile: String)= {

      //读取待分析文档，10个slices
      val originalFileRDD = sc.wholeTextFiles(paths+"/*",8)

      val wholeTextRDD=originalFileRDD.map{
        case (file,stream)=>
          val suffixRegex="""file:(.*)\.(\w+)$""".r
          val (path,suffix)=suffixRegex.findAllIn(file).matchData.map{m=>(m.group(1),m.group(2))}.toMap.head
          val content =  suffix match {
            case "txt"  => stream
            case "docx" => new XWPFWordExtractor(POIXMLDocument.openPackage(path+"."+suffix)).getText
            case _      => 
          }
          (file,content)
      }
      .filter(_._2.toString.nonEmpty)
      .zipWithIndex
      .map{
        case ((file,content),id)=>
        ((id,file.replaceAll("^file:","")),(id,content.toString.replaceAll("\n","")))
      }

      wholeTextRDD.cache()
      
      val pathRDD=wholeTextRDD.keys
      val textRDD=wholeTextRDD.values

      //停词表
      val tokenizer = new SimpleTokenizert(sc, stopwordFile)

      val pretextRDD = textRDD
      //转换成流，作为tokenStream输入参数
        .map{ case (id,line) =>(id, new StringReader(line)) }
        //分词
          .map{ case (id,line) =>(id, new IKAnalyzer(true).tokenStream("",line)) }
          .map{ case (id,y) =>
            val term: CharTermAttribute = y.getAttribute(classOf[CharTermAttribute])
            var newline = ""
            while (y.incrementToken){
              newline += term.toString + " "
            }
           (id, newline)
          }

          //去停词
          val tokenized: RDD[(Long, IndexedSeq[String])] = pretextRDD.map { 
            case (id, text) => id -> tokenizer.getWords(text)
          }

          tokenized.cache()

          //文档中的词频
          val wordCounts: RDD[(String, Long)] = tokenized
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
            val documents = tokenized.map { case (id, tokens) =>
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
            (documents, vocabArray, selectedTokenCount, pathRDD)
            //(documents, vocabArray, selectedTokenCount)
    }
}

//Serializable 类序列化,因为传递的函数（方法）或引用的数据（字段）需要是可序列化的。
private class SimpleTokenizert(sc: SparkContext, stopwordFile: String) extends Serializable {

  private val stopwords: Set[String] = if (stopwordFile.isEmpty) {
    Set.empty[String]
  } else {
    val stopwordText = sc.textFile(stopwordFile).collect()
    stopwordText.flatMap(_.stripMargin.split("\\s+")).toSet
  }
  private val allWordRegex = "^(\\p{L}*)$".r
  private val minWordLength = 3

  //去停词
  def getWords(text: String): IndexedSeq[String] = {

    val words = new mutable.ArrayBuffer[String]()
    val wb = BreakIterator.getWordInstance
    wb.setText(text)

    var current = wb.first()
    var end = wb.next()
    while (end != BreakIterator.DONE) {
      val word: String = text.substring(current, end).toLowerCase
      word match {
        case allWordRegex(w) if w.length >= minWordLength && !stopwords.contains(w) =>
          words += w
        case _ =>
      }

      current = end
      try {
        end = wb.next()
      } catch {
        case e: Exception =>
          end = BreakIterator.DONE
      }
    }
    words
  }
}
