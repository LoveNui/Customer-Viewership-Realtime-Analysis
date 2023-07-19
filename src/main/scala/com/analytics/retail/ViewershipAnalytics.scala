package com.analytics.retail
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types._
import org.apache.spark.streaming.StreamingContext;
import org.apache.spark.streaming._;
import java.io._
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import java.lang._
import org.apache.spark.SparkDriverExecutionException
import org.json4s.{DefaultFormats,jackson}
//import org.elasticsearch.spark.streaming._
import org.elasticsearch.spark.sql._
import org.elasticsearch.spark._


object ViewershipAnalytics {
  def main(args:Array[String])
  {
    val spark = SparkSession
      .builder()
      .appName("US WEB CUSTOMER REALTIME").config("es.nodes", "localhost").config("es.port","9200")
      .config("es.index.auto.create","true")
      .config("es.mapping.id","custid")
      .enableHiveSupport
      .master("local[*]")
      .getOrCreate()
    val sc=spark.sparkContext
    sc.setLogLevel("error")
    import spark.implicits._
    //val objmask=new org.inceptez.framework.masking;
    //spark.udf.register("posmaskudf",objmask.posmask _)
    //val xmldata =sc.textFile("file:///home/hduser/install/usjon/USJSON_PROJECT_CONTENT/http_status.xml")

    println("xml data")
    println("xml data")

    val dfxml = spark.read
      .format("com.databricks.spark.xml")
      .option("rowTag", "httpstatus")
      .load("file:///home/hduser/install/usjon/USJSON_PROJECT_CONTENT/http_status.xml")

    dfxml.cache()

    dfxml.createOrReplaceTempView("statusxmldata")
    spark.sql("select * from statusxmldata").show

    val weblogschema = StructType(Array(
      StructField("username", StringType, true),
      StructField("ip", StringType, true),
      StructField("dt", StringType, true),
      StructField("day", StringType, true),
      StructField("month", StringType, true),
      StructField("time1", StringType, true),
      StructField("yr", StringType, true),
      StructField("hr", StringType, true),
      StructField("mt", StringType, true),
      StructField("sec", StringType, true),
      StructField("tz", StringType, true),
      StructField("verb", StringType, true),
      StructField("page", StringType, true),
      StructField("index", StringType, true),
      StructField("fullpage", StringType, true),
      StructField("referrer", StringType, true),
      StructField("referrer2", StringType, true),
      StructField("statuscd", StringType, true)));

    val weblogrdd= sc.textFile("file:///home/hduser/install/usjon/USJSON_PROJECT_CONTENT/WebLog")

    val weblogrow = weblogrdd.map(x=>x.split(",")).map(x=>Row(x(0),x(1),x(2),x(3),x(4),x(5),x(6)
      ,x(7),x(8),x(9),x(10),x(11),x(12),x(13),x(14),x(15),x(15),x(16)))

    val weblogdf=spark.createDataFrame(weblogrow,weblogschema)
    println("Weblog Data")
    println
    weblogdf.show()
    weblogdf.createOrReplaceTempView("weblog")
    spark.sql("select * from weblog").show

    // COUNTRY STATIC DB DATA

    def loaddb:org.apache.spark.sql.DataFrame={
      val sqldf = spark.read.format("jdbc")
        .option("url", "jdbc:mysql://localhost/custdb")
        .option("driver", "com.mysql.jdbc.Driver")
        .option("dbtable", "custprof")
        .option("user", "root")
        .option("password", "root")
        .load()
      return sqldf;
    }

    val sqldf=loaddb;
    sqldf.cache();

    println("DF created with country SQL data")
    sqldf.show()

    //sqldf.rdd.map(x=>x.mkString(",")).saveAsTextFile("file:///C://SqlData")
    println("sql Data extracted")
    sqldf.createOrReplaceTempView("custprof")
    //val broadcastcountry = sc.broadcast(sqldf)

    //println(broadcastcountry.value.count())

    val ssc1 = new StreamingContext(sc, Seconds(30))

    import org.apache.spark.streaming.dstream.ConstantInputDStream;
    val   dynamiclkp=new ConstantInputDStream(ssc1,sc.parallelize(Seq())).window(Seconds(60),Seconds(60))

    dynamiclkp.foreachRDD{
      x=>{
        val x=sqldf;
        x.unpersist;
        val sqldf1=loaddb;

        sqldf1.cache();
        println(sqldf1.count())
        sqldf1.createOrReplaceTempView("custprof")
      }
    }

    import org.apache.spark.sql.functions._

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "usdatagroup",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topics = Array("user_info")
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc1,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )
    //ssc1.checkpoint("hdfs://localhost:54310/user/hduser/usjsonckpt")
    println("Reading data from kafka")
    val streamdata=    stream.map(record => (record.value))

    //streamdata.print()
    streamdata.foreachRDD(rdd=>

      if(!rdd.isEmpty()){
        //val offsetranges=rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        val jsondf =spark.read.option("multiline", "true").option("mode", "DROPMALFORMED").json(rdd)
        try {
          //val userwithid= jsondf.withColumn("results",explode($"results")).select("results[0].username")
          jsondf.printSchema();
          jsondf.createOrReplaceTempView("usdataview")
          //

          /*val maskeddf=spark.sql("""select
explode(results) as res,
info.page as page,
posmaskudf(res.cell,0,4) as cell,
res.name.first as first,
res.dob.age as age,
posmaskudf(res.email,0,5) as email,res.location.city as uscity,res.location.coordinates.latitude as latitude,
res.location.coordinates.longitude as longitude,res.location.country as country,
res.location.state as state,
res.location.timezone as timezone,res.login.username as username
from usdataview """)
maskeddf.show(false)
*/
          val finaldf=  spark.sql(""" select concat(usd.username,day,month,yr,hr,mt,sec) as custid,
                    row_number() over(partition by usd.username order by yr,month,day,hr,mt,sec) as version,
                    usd.page,usd.cell,usd.first,usd.age,usd.email,
                    concat(usd.latitude,usd.longitude) as coordinates,usd.uscity,usd.country,usd.state,usd.username
                    ,cp.age as age,cp.profession as profession,wl.ip,wl.dt,concat(wl.yr,'-',wl.time1,'-',wl.day) as fulldt,
                    wl.verb,wl.page,wl.statuscd,ws.category,ws.desc,case when wl.dt is null then 'new customer' else
                    'existing customer' end as custtype
                      from
                    (select
  explode(results) as res,
  info.page as page,res.cell as cell,
  res.name.first as first,
  res.dob.age as age,
  res.email as email,res.location.city as uscity,res.location.coordinates.latitude as latitude,
  res.location.coordinates.longitude as longitude,res.location.country as country,
  res.location.state as state,
  res.location.timezone as timezone,res.login.username as username
 from usdataview where info.page is not null) as usd left outer join custprof cp on (substr(regexp_replace(cell,'[()-]',''),0,5)=cp.id)
 left outer join weblog wl on (wl.username=substr(regexp_replace(cell,'[()-]',''),0,5))
 left outer join statusxmldata ws on (wl.statuscd=ws.cd)""")

          finaldf.show(false)

          finaldf.saveToEs("usdataidx/usdatatype")

          println("data written to ES")
          /*           StructField("username", StringType, true),
                   StructField("ip", StringType, true),
                   StructField("dt", StringType, true),
                   StructField("day", StringType, true),
                   StructField("month", StringType, true),
                   StructField("time1", StringType, true),
                   StructField("yr", StringType, true),
                   StructField("hr", StringType, true),
                   StructField("mt", StringType, true),
                   StructField("sec", StringType, true),
                   StructField("tz", StringType, true),
                   StructField("verb", StringType, true),
                   StructField("page", StringType, true),
                   StructField("index", StringType, true),
                   StructField("fullpage", StringType, true),
                   StructField("referrer", StringType, true),
                   StructField("referrer2", StringType, true),
                   StructField("statuscd", StringType, true))); */
          //userwithid.show(false)

          /* val userwithoutid= jsondf.withColumn("results",explode($"results")).select("results.user.username")
           .withColumn("username",regexp_replace(col("username"),  "([0-9])", "")).show()
           */

          /*                  val newjsondf= jsondf.withColumn("results",explode($"results")).select("nationality",

                                "seed","version","results.user.username","results.user.location.city",

                                "results.user.location.state","results.user.location.street","results.user.location.zip",

                                "results.user.md5","results.user.gender","results.user.name.first","results.user.name.last","results.user.name.title","results.user.password",

                                "results.user.phone","results.user.picture.large","results.user.picture.medium","results.user.picture.thumbnail",

                                "results.user.registered","results.user.salt","results.user.sha1","results.user.sha256")
                                .withColumn("username",regexp_replace(col("username"),  "([0-9])", "")).withColumn("phone",regexp_replace(col("phone"), "([(,),-])", "")).withColumn("time_stamp", lit(current_timestamp()))

                                newjsondf.show(false)
          */
          //newjsondf.rdd.coalesce(1).map(x=>x.mkString(",")).saveAsTextFile("file:///home/hduser/usdatastreamdata")

          //s        streamdata.asInstanceOf[CanCommitOffsets].commitAsync(offsetranges)
        }

        catch {
          case ex1: java.lang.IllegalArgumentException => {
            println("Illegal arg exception")
          }

          case ex2: java.lang.ArrayIndexOutOfBoundsException => {
            println("Array index out of bound")
          }

          case ex3: org.apache.spark.SparkException => {
            println("Spark common exception")

          }

          case ex6: java.lang.NullPointerException => {
            println("Values Ignored")

          }
        }
      }

    )

    ssc1.start()
    ssc1.awaitTermination()

  }
}

