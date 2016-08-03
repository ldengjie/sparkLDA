package com.sugon.spark.TopicLDA

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
import org.elasticsearch.spark._
import scala.io.Source

//单例对象，存放工具函数或常量，不能提供构造器参数
object LDATest {

  var ldacontent = ""
  var contentMap = Map[Long, String]()
  var outputlist: List[mutable.Map[String,String]] = List()

  //样例类,自动具备 apply,unapply,toString,equals,hashCode,copy
  private case class Params(
    input: Seq[String] = Seq("D:\\cjsldaresult"),
    k: Int = 50,                         
    maxIterations: Int = 20,             
    docConcentration: Double = -1,      
    topicConcentration: Double = -1,    
    vocabSize: Int = 30000,      
    stopwordFile: String = "",        
    algorithm: String = "em",          
    checkpointDir: Option[String] = None,
    checkpointInterval: Int = 50,     
    esoutput: String = "model/ldaresult6",
    esnodes: String = "192.168.59.128",
    esport: String = "9200") extends AbstractParams[Params]

  def main(args: Array[String]) {
    //待分析文档
    val inputdic = "D:\\8-project\\SogouC.reduced\\Reduced\\C000008"
    //读取文档到变量 ldacontent:一篇文章一行;contentMap:一篇文章一个Map[,String]
    walk(new File(inputdic))
    //把合并后的文档落地
    val ldawriter = new PrintWriter(new File("D:\\testldapre\\ldaresult.txt"),"UTF-8")
    ldawriter.write(ldacontent)
    ldawriter.close()
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
    val (corpus, vocabArray, actualNumTokens) =
      preprocess(sc, params.input, params.vocabSize, params.stopwordFile)
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

        val topicIndices = ldaModel.describeTopics(maxTermsPerTopic = 10)
        val topics = topicIndices.map { case (terms, termWeights) =>
          terms.zip(termWeights).map { case (term, weight) => (vocabArray(term.toInt), weight) }
        }


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

        if (ldaModel.isInstanceOf[DistributedLDAModel]) {
          val distLDAModel = ldaModel.asInstanceOf[DistributedLDAModel]
          val avgLogLikelihood = distLDAModel.logLikelihood / actualCorpusSize.toDouble
          println(s"\t Training data average log likelihood: $avgLogLikelihood")
          val tModel = distLDAModel.topicDistributions.sortByKey().collect().map{
            case(key,value) =>
              for(i<-0 to value.toArray.length-1)
                if(value.toArray(i) == value.toArray.max){
                  val eachline: mutable.Map[String, String] = Map("document" -> key.toString, "content" -> contentMap(key).toString, "keywords" -> topicMap(i).toString)
                  outputlist = outputlist.::(eachline)
                }
          }
          println()
        }

        sc.stop()
  }

  private def walk(file:File) {

    if(file.isFile()) {
      val fileindic = file.getName.substring(0,file.getName.length-4)
      var newfile = fileindic + ", "
      var contentfile = ""
      for (line <- Source.fromFile(file.getPath).getLines){
        newfile += line
        contentfile += line + "\n"
      }
      //一篇文章一行
      ldacontent += newfile + "\n"
      //一篇文章一个Map[,String]
      contentMap += (fileindic.toLong -> contentfile)
    } else
      file.listFiles().foreach(walk)

  }

  private def preprocess(
    sc: SparkContext,
    paths: Seq[String],
    vocabSize: Int,
    stopwordFile: String): (RDD[(Long, Vector)], Array[String], Long) = {

      val textRDD: RDD[String] = sc.textFile(paths.mkString(","),10)

      val tokenizer = new SimpleTokenizert(sc, stopwordFile)

      val pretextRDD = textRDD
        .map(x => new StringReader(x))
        .map(line => new IKAnalyzer(true).tokenStream("",line))
        .map{ y =>
          val term: CharTermAttribute = y.getAttribute(classOf[CharTermAttribute])
          var newline = ""
          while (y.incrementToken){
            newline += term.toString + " "
          }
          newline
        }

        val tokenized: RDD[(Long, IndexedSeq[String])] = pretextRDD.map{ x =>
          (x.split(" ")(0).toLong -> tokenizer.getWords(x))
        }

        tokenized.cache()

        val wordCounts: RDD[(String, Long)] = tokenized
          .flatMap { case (_, tokens) => tokens.map(_ -> 1L) }
          .reduceByKey(_ + _)

          wordCounts.cache()

          val fullVocabSize = wordCounts.count()
          val (vocab: Map[String, Int], selectedTokenCount: Long) = {
            val tmpSortedWCpre: Array[(String, Long)] = if (vocabSize == -1 || fullVocabSize <= vocabSize) {
              wordCounts.collect().sortBy(-_._2)
            } else {
              wordCounts.sortBy(_._2, ascending = false).take(vocabSize)
            }
            val tmpSortedWC = tmpSortedWCpre.filter(_._2>5)
            tmpSortedWC.foreach(x =>
                println(x._1,x._2)
                )
            (tmpSortedWC.map(_._1).zipWithIndex.toMap, tmpSortedWC.map(_._2).sum)
          }

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

          val vocabArray = new Array[String](vocab.size)
          vocab.foreach { case (term, i) => vocabArray(i) = term }
          (documents, vocabArray, selectedTokenCount)
    }
}

private class SimpleTokenizert(sc: SparkContext, stopwordFile: String) extends Serializable {

  private val stopwords: Set[String] = if (stopwordFile.isEmpty) {
    Set.empty[String]
  } else {
    val stopwordText = sc.textFile(stopwordFile).collect()
    stopwordText.flatMap(_.stripMargin.split("\\s+")).toSet
  }
  private val allWordRegex = "^(\\p{L}*)$".r
  private val minWordLength = 3

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
