package de.dhrng.ml.recomm

import com.typesafe.scalalogging.LazyLogging
import de.dhrng.ml.recomm.reader.TranslogReader
import de.dhrng.ml.recomm.transformer.{FilterTransformer, FrequentItemSetTransformer, TransitionProbabilitiesTransformer}
import org.apache.spark.sql.SparkSession

object RecommRunner extends LazyLogging {

  val IMPORT_FOLDER = "data"
  val appName: String = "recomm"
  val master: String = "local[*]"

  def main(args: Array[String]): Unit = {
    run()

  }

  def run(): Unit = {
    val session = createSparkSesion()
    
    val translogs = TranslogReader.from(IMPORT_FOLDER)
      .withSeparator("|")
      .read(session)

    val filterTransformer = new FilterTransformer(session)
    val frequentItemSetTransformer = new FrequentItemSetTransformer(session)
    val transitionProbabilitiesTransformer = new TransitionProbabilitiesTransformer(session, 0.00000000000000000001)

    val byTransaction = filterTransformer.transform(translogs)
    val itemFrequency = frequentItemSetTransformer.transform(byTransaction)

    val transitionProbabilities = transitionProbabilitiesTransformer.transform(itemFrequency)

    val rows = transitionProbabilities.collect

    println("0,9: " + rows.count(_.getDouble(2) > 0.9))
    println("0,8: " + rows.count(_.getDouble(2) > 0.8))
    println("0,7: " + rows.count(_.getDouble(2) > 0.7))
    println("0,6: " + rows.count(_.getDouble(2) > 0.6))
    println("0,5: " + rows.count(_.getDouble(2) > 0.5))
    println("0,4: " + rows.count(_.getDouble(2) > 0.4))
    println("0,3: " + rows.count(_.getDouble(2) > 0.3))
    println("0,2: " + rows.count(_.getDouble(2) > 0.2))
    println("0,1: " + rows.count(_.getDouble(2) > 0.1d))
    println("0,01: " + rows.count(_.getDouble(2) > 0.01d))
    println("0,001: " + rows.count(_.getDouble(2) > 0.001d))
    println("0,0001: " + rows.count(_.getDouble(2) > 0.0001d))
    println("0,00001: " + rows.count(_.getDouble(2) > 0.00001d))
    println("0,000001: " + rows.count(_.getDouble(2) > 0.000001d))
    println("0,0000001: " + rows.count(_.getDouble(2) > 0.0000001d))
    println("0,00000001: " + rows.count(_.getDouble(2) > 0.00000001d))
    println("0,000000001: " + rows.count(_.getDouble(2) > 0.000000001d))
    println("0,0000000001: " + rows.count(_.getDouble(2) > 0.0000000001d))
    println("0,00000000001: " + rows.count(_.getDouble(2) > 0.00000000001d))
    println("0,000000000001: " + rows.count(_.getDouble(2) > 0.000000000001d))
    println("0,0000000000001: " + rows.count(_.getDouble(2) > 0.0000000000001d))

     /*
    val pipeline = new Pipeline()
      .setStages(Array(
        filterTransformer,
        frequentItemSetTransformer,
        transitionProbabilitiesTransformer
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
