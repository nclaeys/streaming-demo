import java.nio.file.{Files, Paths}

import be.axxes.streamingdemo.domain.stream.Played
import be.axxes.streamingdemo.producer.AvroSongProducer.getClass
import org.apache.spark.sql.{SparkSession, avro}
import org.apache.spark.sql.types.{DataTypes, StructType}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.OutputMode

import scala.io.Source

object StreamingApp {

  def main(args: Array[String]): Unit = {
    implicit val sparkSession: SparkSession = SparkSession.builder()
      .appName("music player3")
      .master("local[*]").getOrCreate()
    sparkSession.sparkContext.setLogLevel("ERROR")

    import sparkSession.implicits._

    val inputDf = sparkSession.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "songs-played")
      .load()
    //no deserializers can be configured, should be dataframe operations

    val source = Source.fromURL(this.getClass.getResource("/played.avsc"))
    val schema: String = source.mkString

    val playedSongsDf = inputDf.select(avro.from_avro($"value", schema).as("played"))
      .withColumn("ts", lit(current_timestamp()))
      .select("played.*", "ts")

    playedSongsDf.printSchema()

    val aggregateTitle = playedSongsDf
    .withWatermark("ts", "1 minute")
    //window of 1 minutes sliding every 1 minutes
    .groupBy($"title")


    val outputConsole = aggregateTitle
      .count()
      .writeStream
      .queryName("testQuery")
      .outputMode("complete")
      .format("console")
      .start()

    /*val outputKafka = playedSongsDf.writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("topic", "played-by-minute")
      .option("checkpointLocation", "/opt/spark/streaming-app/checkpoints/song_count")*/

    outputConsole.awaitTermination()


  }
}
