import be.axxes.streamingdemo.domain.stream.Played
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DataTypes, StructType}
import org.apache.spark.sql.functions._

object StreamingApp {

  def main(args: Array[String]): Unit = {
    implicit val sparkSession: SparkSession = SparkSession.builder().appName("music player").master("local[*]").getOrCreate()

    import sparkSession.implicits._

    val inputDf = sparkSession.readStream.format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "playlist")
      .load()
      .selectExpr("CAST(value as STRING)")
    //no deserializers can be configured, should be dataframe operations

    val struct = new StructType()
      .add("sequence", DataTypes.IntegerType)
      .add("dj", DataTypes.StringType)
      .add("title", DataTypes.StringType)
      .add("artist", DataTypes.StringType)

    val playedSongsDf = inputDf.select(from_json($"value", struct).as("song"))
      .withColumn("ts", lit(current_timestamp()))

    val aggregateTitle = playedSongsDf.withWatermark("ts", "1 mminute")
      //window of 1 minutes sliding every 1 minutes
      .groupBy(window($"ts", "1 minute", "1 minute"), $"song.title")

    val outputConsole = playedSongsDf.writeStream
      .outputMode("append")
      .format("console")
      .start()

    val outputKafka = playedSongsDf.writeStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("topic","song_count")
        .option("checkpointLocation", "/opt/spark/streaming-app/checkpoints/song_count")

    outputConsole.awaitTermination()


  }
}
