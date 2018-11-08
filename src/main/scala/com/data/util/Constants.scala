package com.data.util

import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}

object Constants {

  val kafka_cluster1 = Map[String, Object](
    "bootstrap.servers" -> "url:port",
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "group.id" -> "Dwd_Flow",
    "auto.offset.reset" -> "latest",
    "enable.auto.commit" -> (false: java.lang.Boolean),
    "security.protocol" -> "SASL_PLAINTEXT",
    "sasl.mechanism" -> "GSSAPI",
    "sasl.kerberos.service.name" -> "kafka",
    "key.serializer" -> classOf[StringSerializer],
    "value.serializer" -> classOf[StringSerializer] ,
    "offsets.storage" -> "kafka",
    "offsets.storage" -> "kafka",
  "session.timeout.ms" -> "30000",
  "heartbeat.interval.ms" -> "10000"
  )

  val kafka_cluster2 = Map[String, Object](
    "bootstrap.servers" ->   "node1.com:9092",
    "key.serializer" -> classOf[StringSerializer],
    "value.serializer" -> classOf[StringSerializer] ,
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "auto.offset.reset" -> "latest",
    "enable.auto.commit" -> (true: java.lang.Boolean),
    "group.id" -> "todo_topic"

  )

  val kafka_cluster3 = Map[String, Object](
    "bootstrap.servers" ->   "node2.com:9092",
    "key.serializer" -> classOf[StringSerializer],
    "value.serializer" -> classOf[StringSerializer],
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "auto.offset.reset" -> "latest",
    "enable.auto.commit" -> (true: java.lang.Boolean),
    "group.id" -> "spm_refresh000"

  )

  val pv_KafkaConsumserParams = Map[String, Object](
    "bootstrap.servers" ->   "node3.com:9092",
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "group.id" -> "PvTest_Sanity",
    "auto.offset.reset" -> "latest",
    "enable.auto.commit" -> (false: java.lang.Boolean),
    "security.protocol" -> "SASL_PLAINTEXT",
    "sasl.mechanism" -> "GSSAPI",
    "sasl.kerberos.service.name" -> "kafka",
    "key.serializer" -> classOf[StringSerializer],
    "value.serializer" -> classOf[StringSerializer] ,
    "offsets.storage" -> "kafka",
    "session.timeout.ms" -> "30000",
    "heartbeat.interval.ms" -> "10000"
  )

  // rawlog分隔符
  val ORIGINAL_SEPARATOR = '\u0003';

  val pv_test_hive_table = "dwd_pv_di_test"
  val pv_test_hive_table2 = "dwd_pv_di_testtt"
  val pv_test_hive_table3 = "dwd_pv_di_test_ttt"
  val pv_test_hive_table4 = "dwd_pv_di_test_tttt"

  val ev_test_hive_table = "dwd_ev_di_testtt"

  val action_test_hive_table = "dwd_action_di_testtt"


  /** topics to fetch data **/
  val topic_spm_pv = "logcollect_wap_pv"

  /** new topics to write **/
  val topic_pv_dwd = "logcollect_wap_pv_dwd"


}
