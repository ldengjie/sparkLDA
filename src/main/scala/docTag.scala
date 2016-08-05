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
//import org.elasticsearch.spark._
import scala.io.Source

import org.apache.poi.POIXMLDocument;
import org.apache.poi.POITextExtractor; 
import org.apache.poi.xwpf.extractor.XWPFWordExtractor; 
import org.apache.poi.xwpf.usermodel.XWPFDocument; 

//单例对象，存放工具函数或常量，不能提供构造器参数
object LDATest {

  var ldacontent = ""
  var contentMap = Map[Long, String]()
  var outputlist: List[mutable.Map[String,String]] = List()

  //样例类,自动具备 apply,unapply,toString,equals,hashCode,copy 方法
  private case class Params(
    input: Seq[String] = Seq("/root/workSpark/sparkLDA/data/ldaresult.txt"),
    k: Int = 50,                         
    maxIterations: Int = 20,             
    docConcentration: Double = -1,      
    topicConcentration: Double = -1,    
    vocabSize: Int = 30000,      
    stopwordFile: String = "",        
    algorithm: String = "em",          
    checkpointDir: Option[String] = None,
    checkpointInterval: Int = 50
    //,     
    //esoutput: String = "model/ldaresult6",
    //esnodes: String = "192.168.59.128",
    //esport: String = "9200"
  ) extends AbstractParams[Params]

  def main(args: Array[String]) {

    //待分析文档
    val inputdic = "/root/workSpark/sparkLDA/data/SogouC.reduced/Reduced/C000008"

    //遍历所有文档，合成一个总的文档,读取文档到变量 ldacontent:一篇文章一行;contentMap:一篇文章一个Map[,String]
    walk(new File(inputdic))
    //把合并后的文档落地,由 Params.input 读取
    val ldawriter = new PrintWriter(new File("/root/workSpark/sparkLDA/data/ldaresult.txt"),"UTF-8")
    ldawriter.write(ldacontent)
    ldawriter.close()
    //运行LDA
    run(Params())
  }

  private def run(params: Params) {

    val conf = new SparkConf().setAppName(s"LDAExample with $params").setMaster("local[2]")
    conf.set("es.index.auto.create", "true")
    conf.set("pushdown","true")
    //conf.set("es.nodes" , params.esnodes)
    //conf.set("es.port", params.esport)

    val sc = new SparkContext(conf)
    Logger.getRootLogger.setLevel(Level.WARN)
    val preprocessStart = System.nanoTime()
    //准备语料库id+document稀疏矩阵（分词后去掉了停词），词库数组，词数量
    val (corpus, vocabArray, actualNumTokens) =
      preprocess(sc, params.input, params.vocabSize, params.stopwordFile)
    corpus.cache()
    //输出语料信息
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
        //println(outputlist)

        sc.stop()
  }

  //遍历所有文档，合成一个总的文档,读取文档到变量 ldacontent:一篇文章一行;contentMap:一篇文章一个Map[,String]
  private def walk(file:File) {

    if(file.isFile()) {

      var newfile = ""
      var contentfile = ""

      //读取.txt文档名字
      val fileindic = file.getName.substring(0,file.getName.length-4)
      newfile = fileindic + ", "
      //读取.txt文档
      for (line <- Source.fromFile(file.getPath,"GBK").getLines){
        newfile += line
        contentfile += line + "\n"
      }

      //读取.docx文档名字
      //val fileindic = file.getName.substring(0,file.getName.length-5)
      //newfile = fileindic + ", "
      //读取.docx文档
      //contentfile = new XWPFWordExtractor(POIXMLDocument.openPackage("/root/workSpark/sparkLDA/data/87.docx")).getText.replaceAll("\n"," ")
      //newfile+=contentfile
      
      //一篇文章一行
      ldacontent += newfile + "\n"
      //一篇文章一个Map[,String]
      contentMap += (fileindic.toLong -> contentfile)
    } else {
      //println(file.getName)
      file.listFiles().foreach(walk)
    }

  }

  //准备语料库id+document稀疏矩阵（分词后去掉了停词），词库数组，词数量
  private def preprocess(
    sc: SparkContext,
    paths: Seq[String],
    vocabSize: Int,
    stopwordFile: String): (RDD[(Long, Vector)], Array[String], Long) = {

      //读取待分析文档，10个slices
      val textRDD: RDD[String] = sc.textFile(paths.mkString(","),10)

      //停词表
      val tokenizer = new SimpleTokenizert(sc, stopwordFile)

      val pretextRDD = textRDD
      //转换成流，作为tokenStream输入参数
        .map(x => new StringReader(x))
        //分词
          .map(line => new IKAnalyzer(true).tokenStream("",line))
          .map{ y =>
            val term: CharTermAttribute = y.getAttribute(classOf[CharTermAttribute])
            var newline = ""
            while (y.incrementToken){
              newline += term.toString + " "
            }
            newline
          }

          //去停词
          val tokenized: RDD[(Long, IndexedSeq[String])] = pretextRDD.map{ x =>
            (x.split(" ")(0).toLong -> tokenizer.getWords(x))
          }

          tokenized.cache()

          //文档中的词频
          val wordCounts: RDD[(String, Long)] = tokenized
            .flatMap { case (_, tokens) => tokens.map(_ -> 1L) }
            .reduceByKey(_ + _)

            wordCounts.cache()

            //取前 vocabSize 个词，然后要求词频>5，生成词库列表[word，Index]，词的数量
            val fullVocabSize = wordCounts.count()
            val (vocab: Map[String, Int], selectedTokenCount: Long) = {
              val tmpSortedWCpre: Array[(String, Long)] = if (vocabSize == -1 || fullVocabSize <= vocabSize) {
                wordCounts.collect().sortBy(-_._2)
              } else {
                wordCounts.sortBy(_._2, ascending = false).take(vocabSize)
              }
              val tmpSortedWC = tmpSortedWCpre.filter(_._2>5)
              //tmpSortedWC.foreach(x =>
                  //println(x._1,x._2)
                  //)
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
            (documents, vocabArray, selectedTokenCount)
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
