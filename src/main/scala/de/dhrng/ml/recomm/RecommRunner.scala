package de.dhrng.ml.recomm

import com.typesafe.scalalogging.LazyLogging
import de.dhrng.ml.recomm.reader.TranslogReader
import de.dhrng.ml.recomm.transformer.{FilterTransformer, FrequentItemSetTransformer, TransitionProbabilitiesTransformer}
import org.apache.spark.sql.SparkSession

object RecommRunner extends LazyLogging {

  val IMPORT_FOLDER = "data/singleTransaction"
  val appName: String = "recomm"
  val master: String = "local[*]"

  def main(args: Array[String]): Unit = {
    run()

  }

  def run() = {
    val session = createSparkSesion()
    val translogs_df = TranslogReader.from(IMPORT_FOLDER)
      .withSeparator("|")
      .read(session)

    val count: Long = translogs_df
      .select("transactID")
      .groupBy("transactID")
      .count() // counter for groupBy --> entries per transaction IDs
      .count();

    val count2: Long = translogs_df
      .rdd // create RDD
      .map(row => (row.get(2), 1)) // map transaction ID with value one
      .reduceByKey((a, b) => a + b) // reduce by transaction ID and sum values
      .count() // count lines

    println(">>> DF line count: " + count); // count lines
    println(">>> DF line count2: " + count2); // count lines

    val filterTransformer = new FilterTransformer(session)
    val frequentItemSetTransformer = new FrequentItemSetTransformer(session)
    val transitionProbabilitiesTransformer = new TransitionProbabilitiesTransformer(session, 0.000000001)

    val byTransaction = filterTransformer.transform(translogs_df)
    val itemFrequency = frequentItemSetTransformer.transform(byTransaction)
    val transitionProbabilities = transitionProbabilitiesTransformer.transform(itemFrequency);

    //val rows = itemFrequency.collectAsList
    val rows = transitionProbabilities.collectAsList

    /*
    val pipeline = new Pipeline()
      .setStages(Array(
        new FilterTransformer,
        new FrequentItemSetTransformer
      ))

    val model = pipeline.fit(translogs_df)
      */
    session.stop()
  }

  def createSparkSesion(): SparkSession = {
    SparkSession
      .builder()
      .appName(appName)
      .master(master)
      .getOrCreate()
  }

}
