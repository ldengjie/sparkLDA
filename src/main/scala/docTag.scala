package com.sugon.spark.LDA

import java.io.{PrintWriter, File, StringReader,FileInputStream}
import java.text.BreakIterator
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute
import org.wltea.analyzer.lucene.IKAnalyzer
import scala.collection.mutable
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.mllib.clustering._
import org.apache.spark.mllib.linalg.{Vector, Vectors,SparseVector}
import org.apache.spark.mllib.feature.{HashingTF, IDF}
import org.apache.spark.rdd.RDD
import scala.io.Source
import org.elasticsearch.spark._
//office
import org.apache.poi.POITextExtractor   
import org.apache.poi.extractor.ExtractorFactory
import org.apache.poi.hwpf.OldWordFileFormatException
//.pdf
import org.apache.pdfbox.pdmodel.PDDocument
import org.apache.pdfbox.text.PDFTextStripper

object LDATest {

  //样例类,自动具备 apply,unapply,toString,equals,hashCode,copy 方法,可print。用普通类的主构造器也可以实现，不过字段前要加val/var，否则变量没有被使用，不会升为字段
  private case class Params(
    input: String = "file:///spark/work/sparkLDA/data/diShui/doc/1/*",
    k: Int = 50,                         
    //maxIterations: Int = 100,             
    docConcentration: Double = -1,      
    topicConcentration: Double = -1,    
    vocabSize: Int = 30000,      
    algorithm: String = "em",          
    checkpointDir: Option[String] = None,
    checkpointInterval: Int = 50 ,     
    esoutput: String = "lidj/test",
    esnodes: String = "node1",
    esport: String = "9200"
  )

  def main(args: Array[String]) {
    run(Params())
  }

  private def run(params: Params) {

    val conf = new SparkConf().setAppName(s"LDAExample with $params")//.setMaster("local[2]")
    conf.set("es.index.auto.create", "true")
    conf.set("spark.executor.instances", "2")
    conf.set("pushdown","true")
    conf.set("es.nodes" , params.esnodes)
    conf.set("es.port", params.esport)

    val sc = new SparkContext(conf)
    Logger.getRootLogger.setLevel(Level.WARN)
    val preprocessStart = System.nanoTime()
    val (corpus, vocabArray, actualNumTokens, pathRdd,titleRdd,contentRdd,termRdd) = preprocess(sc, params.input, params.vocabSize)
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


    val hashingTF = new HashingTF(Math.pow(2, 18).toInt)  
    //val hashingTF = new HashingTF()  
    //这里将每一行的行号作为doc id，每一行的分词结果生成tf词频向量  
    val id_tf_pairs = termRdd.map {  
      case (id, terms) =>  
        val tf = hashingTF.transform(terms)  
        (id, tf)  
    }  
    id_tf_pairs.cache()  
    //构建idf model  
    val idf = new IDF().fit(id_tf_pairs.values)  
    //将tf向量转换成tf-idf向量  
    val id_tfidf_pairs = id_tf_pairs.mapValues(v => idf.transform(v))  

    val hashIndex_term=termRdd.values.flatMap(_.toList).distinct.map{
      term=>(hashingTF.indexOf(term),term)
    }.collectAsMap()
    //hashIndex_term.foreach(println)
    //println(hashIndex_term.get(46012).mkString)
    //id_tfidf_pairs.foreach(println)

    val termsShowedPerDocFromTfidf=4
    val tfidfRdd=id_tfidf_pairs.map{
      case (id,tfidfVector)=>{
        val sv = tfidfVector.asInstanceOf[SparseVector]  
        val topTerms=sv.values.zip(sv.indices).sortWith(_._1 >_._1).take(Math.min(termsShowedPerDocFromTfidf,sv.indices.size)).toMap.map{
          case(value,index)=>{
            hashIndex_term.get(index).mkString
          }
        }
        (id, topTerms)
      }
    }
    //.foreach(println)


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
        val topicIndices = ldaModel.describeTopics(maxTermsPerTopic = 15)
        val topics = topicIndices.map { case (terms, termWeights) =>
          terms.zip(termWeights).map { case (term, weight) => (vocabArray(term.toInt), weight) }
        }


        //topic的关键词
        import scala.collection.mutable.Map
        var topicMap = topics.zipWithIndex.map{
          case (topic,i)=>{
            (i,topic.toMap.keys.mkString(","))
          }
        }.toMap

        topicMap.foreach(println)

        //保存数据到es
        val keywordNum=8
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
            .join(tfidfRdd)
            .map{
              case (id,(((((rate,topicId),path),title),content),tfidf)) =>{
                //var keywords=tfidf.mkString(",")+","
                var keywords=""
                tfidf.foreach{
                  term=>{
                    if(!contained(keywords,term)) keywords+= term + ","
                  }
                }
                topicMap(topicId).split(",").foreach{
                  term=>{
                    if(!contained(keywords,term)) keywords+= term + ","
                  }
                }
                val keywordFinal=keywords.split(",").take(Math.min(keywordNum,keywords.split(",").size)).mkString(",")
                //val titleRe=""".*/(.+)\.\w+$""".r
                val titleRe1="""《.*?[^》]通知》""".r
                val titleRe2="""关于.*?(通知|请示|意见|函)""".r
                val titleRe3=""".*(通知|请示|意见|函|号|纪要|安排表|处理表|编发)""".r
                val titleRe4="""《.*?》""".r
                val titleRe5=""".*? """.r
                var titleStr=titleRe1.findFirstIn(content).getOrElse(
                  titleRe2.findFirstIn(content).getOrElse(
                    titleRe3.findFirstIn(content).getOrElse(
                      titleRe4.findFirstIn(content).getOrElse(
                        titleRe5.findFirstIn(content).getOrElse(
                          ""
                        )
                      )
                    )
                  )
                )
                  .replaceAll("内部资料","")
                  .replaceAll("注意保管","")
                  .replaceAll("特急","")
                  .replaceAll("缓 急","")
                  .replaceAll("紧急程度","")
                  .replaceAll("密级","")
                  .replaceAll("《","")
                  .replaceAll("》","")
                  //val titleEnd=content.indexOf(" ")
                  //val titleIndex=if(titleEnd<0) content.length else titleEnd
                  //val titleStr=content.substring(0,titleIndex)

                  Map("document" -> path, "title" -> titleStr, "keywords" ->keywordFinal, "topic" -> topicId.toString, "rate" -> rate.toString, "content" -> content)
              }
            }
            //topicToFile.foreach(println)
            //topicToFile.saveToEs(params.esoutput)
        }
        sc.stop()
  }

  //准备语料库id+document稀疏矩阵（分词后去掉了停词），词库数组，词数量
  private def preprocess(
    sc: SparkContext,
    paths: String,
    vocabSize: Int)= {

      //读取待分析文档，10个slices
      val originalFileRDD = sc.wholeTextFiles(paths)

      val wholeTextRDD=originalFileRDD.map{
        case (file,stream)=>
          val suffixRegex="""file:(.*)\.(\w+)$""".r
          val (path,suffix)=suffixRegex.findAllIn(file).matchData.map{m=>(m.group(1),m.group(2))}.toMap.head
          val filePath=path+"."+suffix
          val officeRegex="""^(doc|docx|xls|xlsx|ppt|pptx)$""".r
          var txtContent:String=""
          suffix match {
            case "txt"  => {
              try{
                txtContent=Source.fromFile(filePath,"GBK").mkString
              }catch{
                case _ :Throwable => {
                  txtContent=Source.fromFile(filePath,"UTF-8").mkString
                }
              }
            }
            case officeRegex(office) => {
              try{
                val inputFile = new File(filePath);   
                val extractor = ExtractorFactory.createExtractor(inputFile);   
                //println("read  office  file: "+filePath) 
                txtContent=extractor.getText()   
              }catch{
                case _ :Throwable => {
                  //println("bad  office  file: "+filePath) 
                }
              }
            }
            case "pdf"  =>{
              try{
                val doc=PDDocument.load(new File(filePath))
                val txtContent=new PDFTextStripper().getText(doc)
                doc.close()
              }catch{
                case _ :Throwable => {
                  //println("bad  pdf  file: "+filePath) 
                }
              }
            }
            case _   => 
          }
          txtContent=txtContent.replaceAll("^\\s*\n","").replaceAll("\\s*\n","\n")
          (file,txtContent)
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
                , content.toString.substring(0,Math.max(0,content.toString.indexOf("\n")))
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

        val pos= Source.fromInputStream(getClass().getClassLoader().getResourceAsStream("pos_ansj_default.dic")).getLines().map{
          line=>{
            val s=line.split("\\s")
            s(0)->s(1)
          }
        }.toMap
        //pos.foreach(println)
        val minWordLength = 2
        val termRDD = contentRDD
        //转换成流，作为tokenStream输入参数
          .map{ case (id,line) =>(id, new StringReader(line)) }
          //分词
            .map{ case (id,line) =>(id, new IKAnalyzer(true).tokenStream("",line)) }
            .map{ case (id,y) =>
              val term: CharTermAttribute = y.getAttribute(classOf[CharTermAttribute])
              var newline = new mutable.ArrayBuffer[String]()
              while (y.incrementToken){
                val t=term.toString
                var posClass=pos.get(t).getOrElse("-").charAt(0).toString
                if (t.length >= minWordLength && (posClass=="n"||posClass=="a")) newline += t
                //if (t.length >= minWordLength && (posClass=="n"||posClass=="a"||posClass=="g"||posClass=="i")) newline += t
              //if (t.length >= minWordLength ) newline += t
              }
              //println(id,newline)
              (id, newline)
            }
            termRDD.cache()

            //文档中的词频
            val wordCounts: RDD[(String, Long)] = termRDD
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
              val documents = termRDD.map { case (id, tokens) =>
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
              (documents, vocabArray, selectedTokenCount, pathRDD,titleRDD,contentRDD,termRDD)
              //(documents, vocabArray, selectedTokenCount)
  }
    private def contained (str1:String,str2:String):Boolean={
      if(str1.nonEmpty){
        str1.split(",").map{ term=>{ term.contains(str2) || str2.contains(term) } } .reduce(_||_)
      }else{
        false
      }
    }
}
