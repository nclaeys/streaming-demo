import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{DataTypes, StructType}

object StreamingCountExampleApp {

  def main(args: Array[String]): Unit = {
    implicit val sparkSession: SparkSession = SparkSession.builder()
      .appName("music player")
      .master("local[*]")
      .getOrCreate()

    sparkSession.sparkContext.setLogLevel("ERROR")
    import sparkSession.implicits._

    val struct = new StructType()
      .add("sequence", DataTypes.IntegerType)
      .add("customerId", DataTypes.StringType)
      .add("title", DataTypes.StringType)
      .add("artist", DataTypes.StringType)
      .add("album", DataTypes.StringType)

    val inputDf = sparkSession.readStream.format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "songs-played-json")
      .load()
      .selectExpr("CAST(value as STRING)")
      .select(from_json($"value", struct).as("played"))
      .withColumn("ts", lit(current_timestamp()))
      .selectExpr("played.*", "ts")

    val aggregateResult = inputDf
      .withWatermark("ts", "1 minute")
      .groupBy(window($"ts", "1 minute", "1 minute"), $"title")
      .count()

    aggregateResult
      .writeStream
      .outputMode("append")
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("topic", "songs-played-by-minute")
      .option("checkpointLocation", "/opt/spark/streaming-app/checkpoints/song_count")
      .trigger(Trigger.Continuous("1 second"))
      .start()
      .awaitTermination()

  }
}
