package de.dhrng.ml.recomm

import java.io.File

import com.typesafe.scalalogging.LazyLogging
import de.dhrng.ml.recomm.transformer.{FilterTransformer, FrequentItemSetTransformer}
import org.apache.spark.ml.Pipeline
import org.apache.spark.sql.{DataFrame, SparkSession}

object RecommRunner extends LazyLogging {

  val IMPORT_FOLDER = "../translogs"
  val appName: String = "recomm"
  val master: String = "local[*]"

  def main(args: Array[String]): Unit = {
    run()

  }

  def run() = {
    val session = createSparkSesion()
    val translogs_df = readTranslogs(session)

    val pipeline = new Pipeline()
      .setStages(Array(
        new FilterTransformer,
        new FrequentItemSetTransformer
      ))

    val model = pipeline.fit(translogs_df)

    session.stop()
  }

  def createSparkSesion(): SparkSession = {
    SparkSession
      .builder()
      .appName(appName)
      .master(master)
      .getOrCreate()
  }

  def readTranslogs(session: SparkSession): DataFrame = {
    // get list fo input data
    logger.info("read list of files: {}", IMPORT_FOLDER)
    val inputFiles = new File(IMPORT_FOLDER).listFiles()
      .toList
      .filter(file => file.getName.endsWith(".csv"))

    // read first file to DataFrame and append all following files
    var translogs_df = readCsv(session, inputFiles(0).getAbsolutePath())
    for (i <- 1 to (inputFiles.length - 1)) {
      translogs_df = translogs_df.union(readCsv(session, inputFiles(i).getAbsolutePath()))
    }

    return translogs_df
  }

  /**
    * read CSV file from path and create a DataFrame
    *
    * path:String path to read CSV file form
    *
    * return DataFrame
    */
  def readCsv(session: SparkSession, path: String): org.apache.spark.sql.DataFrame = {
    val df = session.read.format("CSV") // define file format
      .option("header", "true") // use header line for column names
      .option("sep", "|") // define column separator
      .option("timestampFormat", "yyyy-MM-dd HH:mm:ss") // define date format
      .load(path) // load file form path into DataFrame

    // print debugging output
    logger.debug("read {} lines from: {}", df.count(), path)

    return df
  }


}
