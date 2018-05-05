package de.dhrng.ml.recomm

import com.typesafe.scalalogging.LazyLogging
import de.dhrng.ml.recomm.estimator.ActionValueFunctionEstimator
import de.dhrng.ml.recomm.reader.TranslogReader
import de.dhrng.ml.recomm.transformer.{FilterTransformer, FrequentItemSetTransformer, TransitionProbabilitiesTransformer}
import de.dhrng.ml.recomm.writer.CsvWriter
import org.apache.spark.ml.Pipeline
import org.apache.spark.sql.SparkSession

object RecommRunner extends LazyLogging {

  val appName: String = "recomm"
  val master: String = "local[*]"

  def main(args: Array[String]): Unit = {

    val importFolder = args(0)

    if(importFolder == null || importFolder.isEmpty){
      throw new IllegalArgumentException("path as argument is missing")
    }

    run(importFolder)
  }

  def run(importFolder: String): Unit = {
    val session = createSparkSesion()
    
    val translogs = TranslogReader.from(importFolder)
      .withSeparator("|")
      .read(session)

    val filterTransformer = new FilterTransformer(session)
    val frequentItemSetTransformer = new FrequentItemSetTransformer(session)
    val transitionProbabilitiesTransformer = new TransitionProbabilitiesTransformer(session)
    val actionValueFunctionEstimator = new ActionValueFunctionEstimator(session, episodeEndingDepth = 4)

    val pipeline = new Pipeline()
      .setStages(Array(
        filterTransformer,
        frequentItemSetTransformer,
        transitionProbabilitiesTransformer,
        actionValueFunctionEstimator
      ))

    var model = pipeline.fit(translogs)

    CsvWriter.write(model, "target/action-values.csv")

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
