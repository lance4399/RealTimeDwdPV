package com.data.dwdFlow

import java.net.URI
import java.text.SimpleDateFormat
import java.util.Date

import com.data.util.{Constants, KafkaSink}
import org.apache.spark.mllib.linalg
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.tree.model.RandomForestModel
import org.apache.spark.sql.Row
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}
import com.entity.dwd.DwdShmmWebPvDi
import com.handlers.ods2dwd.Ods2Dwd_WebPvDiHandler
import com.handlers.rawlog2ods.Origin2Ods_PvLogHandler
import com.models.rawlog.OriginalPvLogModel

import scala.util.Try;

object Spark_Pv{
  val sparkConf = new SparkConf().setAppName("PvDwdFlow_batch5s").set("spark.speculation","false")
    .set("spark.streaming.kafka.maxRatePerPartition", "10000")
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .set("spark.scheduler.executorTaskBlacklistTime","30000")
    .set("spark.network.timeout","300000")
    .set("spark.streaming.backpressure.enabled","true")
    .set("spark.streaming.backpressure.initialRate","200")
    .set("spark.streaming.stopGracefullyOnShutdown","true")
    .set("spark.rdd.compress","true")

  val ssc = new StreamingContext(sparkConf, Seconds(5))
  val sc = ssc.sparkContext
}

object PvDwdFlow {
  def main(args: Array[String]) {
    Spark_Pv.ssc.checkpoint("/user/node1/liangxi/tmp/dwdflow/checkpoint_pv_batch5s")
    val originalPvLogModel_broadcast = Spark_Pv.sc.broadcast( OriginalPvLogModel.getOriginalPvLogModel() );
    val newOriginalPvLogHandler_broadcast = Spark_Pv.sc.broadcast( Origin2Ods_PvLogHandler.getNewOriginalPvLogHandler() );
    val odsShmmWebPvDiHandler_broadcast = Spark_Pv.sc.broadcast( Ods2Dwd_WebPvDiHandler.getOdsShmmWebPvDiHandler() );
    val kafkaProducer = Spark_Pv.sc.broadcast(KafkaSink[String, String] (Constants.kafka_cluster2) )

    val date = new SimpleDateFormat("yyyyMMdd").format(new Date().getTime - 1000*3600*24*2 ) //the model is 2 days ago
    val date2 = new SimpleDateFormat("yyyyMMdd").format(new Date().getTime - 1000*3600*24*3 ) //the model is 3 days ago

    val map: Map[String, RandomForestModel] = Map(
      date + "/wap" -> Try(RandomForestModel.load(Spark_Pv.sc, "hdfs://node1/liangxi/" + date + "/wap/randomForest.model"))
        .getOrElse(RandomForestModel.load(Spark_Pv.sc, "hdfs://node1/liangxi/" + date2 + "/wap/randomForest.model")),
      date + "/pc"  -> Try(  RandomForestModel.load(Spark_Pv.sc,  "hdfs://node1/liangxi/" + date + "/pc/randomForest.model") )
        .getOrElse( RandomForestModel.load(Spark_Pv.sc, "hdfs://node1/liangxi/" + date2 + "/pc/randomForest.model") )
    )

    var modelMap_brocast = Spark_Pv.sc.broadcast(map)
    val topics = Array(Constants.topic_spm_pv)
    val kafkaParams = Constants.kafka_cluster1
    val pv_stream = KafkaUtils.createDirectStream[String, String]( Spark_Pv.ssc, PreferConsistent, Subscribe[String, String](topics, kafkaParams))
    //task begin here
    pv_stream.foreachRDD(consumerRecordRDD => {
      try{
        val dt = new SimpleDateFormat("yyyyMMdd").format(new Date().getTime - 1000*3600*24*2 )
        val path_wap = new Path( "hdfs://dc1/user/mediaai/litao/"+ dt +"/wap/randomForest.model/data")
        val path_pc  = new Path( "hdfs://dc1/user/mediaai/litao/"+ dt + "/pc/randomForest.model/data")
        val hdfs = FileSystem.get( new URI("hdfs://dc1"), new Configuration() )
        if ( hdfs.exists(path_wap) && hdfs.exists(path_pc) && !modelMap_brocast.value.contains(dt+"/wap") && !modelMap_brocast.value.contains(dt+"/pc") ) {
          var tmp:Map[String,RandomForestModel] =Map()
          tmp += ( dt +"/wap" -> RandomForestModel.load(Spark_Pv.sc, "hdfs://node1/liangxi/"+ dt + "/wap/randomForest.model") )
          tmp += ( dt +"/pc"  -> RandomForestModel.load(Spark_Pv.sc, "hdfs://node1/liangxi/"+ dt + "/pc/randomForest.model") )
          if(tmp.get(dt + "/wap").get != null && tmp.get(dt +"/pc").get != null ){
            modelMap_brocast.unpersist
            modelMap_brocast = Spark_Pv.sc.broadcast(tmp)
            println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss SSS").format(new Date()) +" modelMap updated!")
          }
        }
      }catch {
        case e: Exception => println(e)
      }
      consumerRecordRDD.filter( _ != null)
        .foreachPartition( partition=>{
          val producer = kafkaProducer.value
          var originalPvLogModel = originalPvLogModel_broadcast.value
          val newOriginalPvLogHandler= newOriginalPvLogHandler_broadcast.value
          val odsShmmWebPvDiHandler =  odsShmmWebPvDiHandler_broadcast.value
          val keyList = modelMap_brocast.value.map(_._1)
          var key_wap:String = null
          var key_pc:String = null
          keyList.foreach(i => {
            if(i.endsWith("wap"))
              key_wap= i
            else if(i.endsWith("pc"))
              key_pc = i
          })
          val model_wap = Try(modelMap_brocast.value.get( key_wap ).get).get
          val model_pc =  Try(modelMap_brocast.value.get( key_pc).get ).get
          partition.foreach(log =>{
            originalPvLogModel.setData( log.value() )
            val odsShmmWebPvDiModel = newOriginalPvLogHandler.parsing (originalPvLogModel) //rawLog ->ods
            if(odsShmmWebPvDiModel.getData != null) {
              val dwdShmmWebPvDiModel = odsShmmWebPvDiHandler.parsing(odsShmmWebPvDiModel) //ods->dwd
              val dwdShmmWebPvDi = dwdShmmWebPvDiModel.getData
              val inputVector = generateFormatedVector(dwdShmmWebPvDi)
              var prediction:Double = 1.0
              if(dwdShmmWebPvDi.getBusiness.equals("wap")){
                prediction = model_wap.predict(inputVector)
//                println("model_wap prediction" )
              }else if(dwdShmmWebPvDi.getBusiness.equals("pc")){
                prediction = model_pc.predict(inputVector)
//                println("model_pc prediction" )
              }
              dwdShmmWebPvDi.setIsAbnormal(prediction)
              producer.send(Constants.topic_pv_dwd,
                            dwdShmmWebPvDi.getSpmCnt+"."+dwdShmmWebPvDi.getPagePvId,
                             dwdShmmWebPvDi.toString)
              //                println(new SimpleDateFormat("yyyyMMdd HH:mm:ss SSS").format(new Date()) + "Msg successfully sent" )
            }
          })
        })
    })
    Spark_Pv.ssc.start()
    Spark_Pv.ssc.awaitTermination()
  }

  def generateFormatedVector(dwdPv: DwdShmmWebPvDi): linalg.Vector = {
    val row =  Row(
      dwdPv.getLogVersion, dwdPv.getLogTime, dwdPv.getLogTimeStamp, dwdPv.getJsVersion, dwdPv.getVstCookie, //5
      dwdPv.getVstDeviceId,  dwdPv.getVstBrowserId, dwdPv.getVstDeviceType, //3
      dwdPv.getVstDeviceModel, dwdPv.getVstDeviceResolution, dwdPv.getVstOsType, dwdPv.getVstBrowserType, //4
      dwdPv.getVstBrowserVersion, dwdPv.getPagePvId, dwdPv.getPageUrl, dwdPv.getPageUrlNoParams, dwdPv.getPageUrlHost, //5
      dwdPv.getPageTypeId, dwdPv.getPageType,  //2
      dwdPv.getReferUrl, dwdPv.getReferUrlNoParams, dwdPv.getReferUrlHost, dwdPv.getReferPvId, dwdPv.getSourceId,
      dwdPv.getSpmCnt, dwdPv.getSpmPre, dwdPv.getScmCnt, dwdPv.getSpmCntA,
      dwdPv.getSpmCntB, dwdPv.getSpmPreA, dwdPv.getSpmPreB, dwdPv.getSpmPreC, dwdPv.getSpmPreD,
      dwdPv.getScmCntA, dwdPv.getScmCntB, dwdPv.getScmCntC, dwdPv.getScmCntD,
      dwdPv.getIsCrawler, dwdPv.getIsAbnormal, dwdPv.getAbnormalRule, dwdPv.getBusiness,dwdPv.getVstUserAgent,
      dwdPv.getExt,dwdPv.getIsNewUser) //44
    val colArray = splitDataFrame(row)
    Vectors.dense(Array(colArray(1),colArray(2),colArray(3),colArray(4),colArray(5),colArray(6),colArray(7),colArray(8)))
  }

  def splitDataFrame(row:Row):Array[Double]={
    var vstIP = -1.0
    var vstDeviceId = -1.0
    var vstBrowserId = -1.0
    var vstDeviceResolution_1 = -1.0
    var vstDeviceResolution_2 = -1.0
    var pageTypeId = -1.0
    var sourceId = -1.0
    var isAbnormal = 0.0
    var isNewUser = -1.0
    if (row.length == 44) {
      if ((row.getString(5) != null) && (!row.getString(5).equals(""))) {
        try {
          val ipList = row.getString(5).split("[.]")
          if (ipList.length >= 4) {
            try {
              vstIP = (ipList(0).toInt * 256 * 256 * 256 + ipList(1).toInt * 256 * 256 + ipList(2).toInt * 256 + ipList(3).toInt).toDouble
            } catch {
              case e: Exception => println(e)
                vstIP = -1.0
            }
          }
        }
      }
      try {
        vstDeviceId = row.getInt(5).toDouble
      } catch {
        case e: Exception => println(e)
          vstDeviceId = -1.0
      }
      try {
        vstBrowserId = row.getInt(6).toDouble
      } catch {
        case e: Exception => println(e)
          vstBrowserId = -1.0
      }
      if ((row.getString(8) != null) && (!row.getString(8).equals(""))) {
        if (row.getString(8).split("_").length >= 2) {
          try {
            vstDeviceResolution_1 = row.getString(8).split("_")(0).toDouble
            vstDeviceResolution_2 = row.getString(8).split("_")(1).toDouble
          } catch {
            case e: Exception => println(e)
              vstDeviceResolution_1 = -1.0
              vstDeviceResolution_2 = -1.0
          }
        }
      }
      try {
        pageTypeId = row.getInt(18).toDouble
      } catch {
        case e: Exception => println(e)
          pageTypeId = -1.0
      }

      if ((row.getString(23) != null) && (!row.getString(23).equals(""))) {
        try {
          sourceId = row.getString(23).toDouble
        } catch {
          case e: Exception => println(e)
            sourceId = -1.0
        }
      }

      try {
        isAbnormal = row.getInt(38).toDouble
      } catch {
        case e: Exception => println(e)
          isAbnormal = -1.0
      }
      try {
        isNewUser = row.getInt(43).toDouble
      } catch {
        case e: Exception => println(e)
          isNewUser = -1.0
      }
    }
    Array(isAbnormal, vstIP / 1000000, vstDeviceId, vstBrowserId, vstDeviceResolution_1, vstDeviceResolution_2,
      pageTypeId, sourceId,  isNewUser)
  }

}
