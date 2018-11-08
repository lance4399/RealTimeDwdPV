package com.data.util

import org.apache.spark.sql.types.{StructField, _}

/**
  * @Author: xiliang
  * @Create: 2018/7/24 20:31
  */
object Schemas {
  val DwdWebPv_Schema=StructType(
    List(
        StructField("logVersion", IntegerType),
        StructField("logTime", LongType),
        StructField("logTimeStamp", LongType),
        StructField("jsVersion", StringType),
        StructField("vstCookie", StringType),    //5

        StructField("vstDeviceId", IntegerType),
        StructField("vstBrowserId", IntegerType),
        StructField("vstDeviceType", StringType), //8

        StructField("vstDeviceResolution", StringType),
        StructField("vstOsType", StringType),
        StructField("vstOsVersion", StringType),
        StructField("vstBrowserType", StringType),//12

        StructField("vstBrowserVersion", StringType),
        StructField("pagePvId", StringType),
        StructField("pageUrl", StringType),
        StructField("pageUrlNoParams", StringType),
        StructField("pageUrlHost", StringType),//17

        StructField("pageTypeId", IntegerType),
        StructField("pageType", StringType),//19

        StructField("referUrl", StringType),
        StructField("referUrlNoParams", StringType),
        StructField("referUrlHost", StringType),
        StructField("referPvId", StringType),
        StructField("sourceId", StringType),//24

        StructField("spmCnt", StringType),
        StructField("spmPre", StringType),
        StructField("scmCnt", StringType),
        StructField("spmCntA", StringType),//28

        StructField("spmCntB", StringType),
        StructField("spmPreA", StringType),
        StructField("spmPreB", StringType),
        StructField("spmPreC", StringType),
        StructField("spmPreD", StringType),//33

        StructField("scmCntA", StringType),
        StructField("scmCntB", StringType),
        StructField("scmCntC", StringType),
        StructField("scmCntD", StringType),//37

        StructField("isCrawler", IntegerType),
        StructField("isAbnormal", DoubleType),
        StructField("abnormalRule", StringType),
        StructField("business", StringType),
        StructField("vstUserAgent", StringType),//42

        StructField("ext", StringType),
        StructField("isNewUser", IntegerType) //44

    )
  )

}
