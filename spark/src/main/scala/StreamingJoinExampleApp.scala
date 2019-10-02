import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{DataTypes, StructType}

import scala.io.Source

object StreamingApp {

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

    val customers = sparkSession.read.format("json")
      .load("src/main/resources/customers.json")

    val streamingDf = sparkSession.readStream.format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "songs-played-json")
      .load()
      .selectExpr("CAST(value as STRING)")
      .select(from_json($"value", struct).as("played"))
      .withColumn("ts", lit(current_timestamp()))
      .selectExpr("played.*", "ts")

    val enrichedResult = streamingDf
      .join(customers, $"customerId" === $"id", "left_outer")
      .na.fill("unknown")
      .groupBy($"country")
      .count()

    enrichedResult
      .writeStream
      .outputMode("complete")
      .format("console")
      .start()
      .awaitTermination()
  }
}
