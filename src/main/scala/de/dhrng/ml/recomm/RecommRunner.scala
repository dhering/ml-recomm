package de.dhrng.ml.recomm

import com.typesafe.scalalogging.LazyLogging
import de.dhrng.ml.recomm.estimator.ActionValueFunctionEstimator
import de.dhrng.ml.recomm.reader.TranslogReader
import de.dhrng.ml.recomm.transformer.{FilterTransformer, FrequentItemSetTransformer, TransitionProbabilitiesTransformer}
import org.apache.spark.ml.Pipeline
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
    val transitionProbabilitiesTransformer = new TransitionProbabilitiesTransformer(session)
    val actionValueFunctionEstimator = new ActionValueFunctionEstimator(session)


    val pipeline = new Pipeline()
      .setStages(Array(
        filterTransformer,
        frequentItemSetTransformer,
        transitionProbabilitiesTransformer,
        actionValueFunctionEstimator
      ))

    val model = pipeline.fit(translogs)
        .stages.last.asInstanceOf[de.dhrng.ml.recomm.estimator.ActionValueModel]

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
