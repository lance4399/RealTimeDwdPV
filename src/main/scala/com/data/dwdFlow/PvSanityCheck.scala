package com.data.dwdFlow

import java.net.URI
import java.text.SimpleDateFormat
import java.util.Date
import com.entity.dwd.DwdShmmWebPvDi
import com.handlers.ods2dwd.Ods2Dwd_WebPvDiHandler
import com.handlers.rawlog2ods.Origin2Ods_PvLogHandler
import com.models.rawlog.OriginalPvLogModel
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkConf
import org.apache.spark.mllib.tree.model.DecisionTreeModel
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import scala.util.Try
import dwdFlow.PvDwdFlow.generateFormatedVector
import util.{Constants, Schemas}

object Spark_PvSanity{
  val sparkConf = new SparkConf().setAppName("Pv_SanityCheck").set("spark.speculation","false")
    .set("spark.streaming.kafka.maxRatePerPartition", "10000")
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .set("spark.scheduler.executorTaskBlacklistTime","30000")
    .set("spark.network.timeout","300000")
  val ssc = new StreamingContext(sparkConf, Seconds(20))
  val sc = ssc.sparkContext
  val sparkSession = SparkSession.builder().config("hive.exec.dynamic.partition",  "true")
    .config("spark.sql.warehouse.dir", "hdfs://masterNode/hive/warehouse/")
    .config("hive.exec.dynamic.partition.mode", "nonstrict").enableHiveSupport().getOrCreate()
}

object PvSanityCheck {
  def main(args: Array[String]): Unit = {

    Spark_PvSanity.ssc.checkpoint("/user/tmp/logCollector/rt_pv2")
    val date = new SimpleDateFormat("yyyyMMdd").format(new Date().getTime - 1000*3600*24*2 ) //the model is 2 days ago
    val map: Map[String, DecisionTreeModel] = Map(
      date +"/wap" -> DecisionTreeModel.load(Spark_PvSanity.sc, "hdfs://node1/user/liangxi/" + date + "/wap/decisionTree.model") )
    var modelMap_brocast = Spark_PvSanity.sc.broadcast(map)
    val topics = Array(Constants.topic_spm_pv)
    val kafkaParams = Constants.pv_KafkaConsumserParams
    val pv_stream = KafkaUtils.createDirectStream[String, String]( Spark_PvSanity.ssc, PreferConsistent, Subscribe[String, String](topics, kafkaParams))
    //task begin here
    pv_stream.foreachRDD(consumerRecordRDD => {
      try{
        val dt = new SimpleDateFormat("yyyyMMdd").format(new Date().getTime - 1000*3600*24*2 )
        val path_wap = new Path( "hdfs://dc1/user/mediaai/litao/"+ dt +"/wap/randomForest.model/data")
        val hdfs = FileSystem.get( new URI("hdfs://dc1"), new Configuration() )
        if (  hdfs.exists(path_wap) && !modelMap_brocast.value.contains(dt.toString) ) {
          var tmp =  modelMap_brocast.value
          Try(tmp += ( dt +"/wap" -> DecisionTreeModel.load(Spark_PvSanity.sc, "hdfs://node1/user/liangxi/"+ dt + "/wap/decisionTree.model") ))
          if( tmp.get(dt +"/wap").get !=null){
            modelMap_brocast.unpersist
            modelMap_brocast = Spark_PvSanity.sc.broadcast(tmp)
            println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss SSS").format(new Date()) +" modelMap updated!")
          }
        }
      }catch {
        case e: Exception => println(e)
      }
      val dwdPvRDD = consumerRecordRDD.filter(_ != null)
        .map( log=>{
          var dwdPv = new DwdShmmWebPvDi
          var originalPvLogModel = OriginalPvLogModel.getOriginalPvLogModel
          originalPvLogModel.setData(log.value() )
          val odsShmmWebPvDiModel = Origin2Ods_PvLogHandler.getNewOriginalPvLogHandler.parsing(originalPvLogModel) //rawLog ->ods
          if(odsShmmWebPvDiModel.getData != null) {
            val dwdShmmWebPvDiModel = Ods2Dwd_WebPvDiHandler.getOdsShmmWebPvDiHandler.parsing(odsShmmWebPvDiModel) //ods->dwd
            dwdPv = dwdShmmWebPvDiModel.getData
            val inputVector = generateFormatedVector(dwdPv)
            val dt2 = new SimpleDateFormat("yyyyMMdd").format(new Date().getTime - 1000*3600*24*3 )
            val dt3 = new SimpleDateFormat("yyyyMMdd").format(new Date().getTime - 1000*3600*24*4 )
            val model_wap = Try( map.get(dt2 +"/wap").get ).getOrElse( map.get(dt3 +"/wap").get )
//            val model_pc = Try( map.get(dt2 +"/pc").get ).getOrElse( map.get(dt3 +"/pc").get )
            val prediction = model_wap.predict(inputVector)
            dwdPv.setIsAbnormal(prediction)
          }
          Row(
            dwdPv.getLogVersion, dwdPv.getLogTime, dwdPv.getLogTimeStamp, dwdPv.getJsVersion, dwdPv.getVstCookie, //5
            dwdPv.getVstIp, dwdPv.getVstCountryId, dwdPv.getVstProvinceId, dwdPv.getVstCityId, dwdPv.getVstUserId, //10
            dwdPv.getVstDeviceId, dwdPv.getVstOsId, dwdPv.getVstBrowserId, dwdPv.getVstDeviceType, dwdPv.getVstDeviceBrand, //15
            dwdPv.getVstDeviceModel, dwdPv.getVstDeviceResolution, dwdPv.getVstOsType, dwdPv.getVstOsVersion, dwdPv.getVstBrowserType, //20
            dwdPv.getVstBrowserVersion, dwdPv.getPagePvId, dwdPv.getPageUrl, dwdPv.getPageUrlNoParams, dwdPv.getPageUrlHost, //25
            dwdPv.getPageTypeId, dwdPv.getPageType, dwdPv.getPageSubTypeId, dwdPv.getPageSubType, dwdPv.getPageYyid, //30
            dwdPv.getReferUrl, dwdPv.getReferUrlNoParams, dwdPv.getReferUrlHost, dwdPv.getReferPvId, dwdPv.getSourceId, //35
            dwdPv.getSourceFirstId, dwdPv.getSourceSecondId, dwdPv.getSourceThirdId, dwdPv.getSourceFourthId, dwdPv.getContentId, //40
            dwdPv.getSohuMediaId, dwdPv.getSpmCnt, dwdPv.getSpmPre, dwdPv.getScmCnt, dwdPv.getSpmCntA, //45
            dwdPv.getSpmCntB, dwdPv.getSpmPreA, dwdPv.getSpmPreB, dwdPv.getSpmPreC, dwdPv.getSpmPreD, //50
            dwdPv.getScmCntA, dwdPv.getScmCntB, dwdPv.getScmCntC, dwdPv.getScmCntD, dwdPv.getSpmBAb, //55
            dwdPv.getSpmCAb, dwdPv.getSpmPreBAb, dwdPv.getSpmPreCAb, dwdPv.getTransCode, dwdPv.getJump, //60
            dwdPv.getIsCrawler, dwdPv.getIsAbnormal, dwdPv.getAbnormalRule, dwdPv.getBusiness,dwdPv.getVstUserAgent,  //65
            dwdPv.getExt,dwdPv.getIsNewUser) //67
        })
      val dwdPvDF = Spark_PvSanity.sparkSession.createDataFrame( dwdPvRDD , Schemas.DwdWebPv_Schema )
      dwdPvDF.createOrReplaceTempView("dwd_pv_tmp")
      Spark_PvSanity.sparkSession.sql("select count(*) from dwd_pv_tmp").show()
      val sql = "insert into table "+ Constants.pv_test_hive_table4 +" partition(dt='" + date +"') select * from dwd_pv_tmp"
      Spark_PvSanity.sparkSession.sql("use mediaai")
      Spark_PvSanity.sparkSession.sql(sql)
    })
    Spark_PvSanity.ssc.start()
    Spark_PvSanity.ssc.awaitTermination()
  }

}
