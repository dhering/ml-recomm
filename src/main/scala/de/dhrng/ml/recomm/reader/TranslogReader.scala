package de.dhrng.ml.recomm.reader

import java.io.{File, FilenameFilter}

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * @author Dennis Hering (dennis.hering@nexum.de)
  */
class TranslogReader private(folder: String) extends LazyLogging {

  var format: String = "csv"
  var filterFileExtension: Boolean = false
  var fileExtension: String = "csv"
  var separator: String = ","
  var header: Boolean = true
  var timestampFormat: String = "yyyy-MM-dd HH:mm:ss"

  def withFormat(format: String): TranslogReader = {
    this.format = format
    this
  }

  def withFileExtensionFilter(fileExtension: String): TranslogReader = {
    this.filterFileExtension = true
    this.fileExtension = fileExtension
    this
  }

  def withSeparator(separator: String): TranslogReader = {
    this.separator = separator
    this
  }

  def withHeader(): TranslogReader = {
    this.header = true
    this
  }

  def withOutHeader(): TranslogReader = {
    this.header = false
    this
  }

  def withTimestampFormat(timestampFormat: String): TranslogReader = {
    this.timestampFormat = timestampFormat
    this
  }

  def read(session: SparkSession): DataFrame = {
    // get list fo input data
    logger.info("read list of files: {}", folder)

    val inputFiles: List[File] = getInputFiles

    // read first file to DataFrame and append all following files
    var translogs = readDataFrame(session, inputFiles.head.getAbsolutePath)
    for (i <- 1 until inputFiles.size) {
      translogs = translogs.union(readDataFrame(session, inputFiles(i).getAbsolutePath))
    }

    // return translogs
    translogs
  }

  private def getInputFiles: List[File] = {
    val files =
      if (filterFileExtension) {
        new File(folder).listFiles(new FileExtensionfilter(fileExtension))
      } else {
        new File(folder).listFiles
      }

    files.toList.filter(file => !file.isDirectory)
  }

  /**
    * read CSV file from path and create a DataFrame
    *
    * path:String path to read CSV file form
    *
    * return DataFrame
    */
  private def readDataFrame(session: SparkSession, path: String): org.apache.spark.sql.DataFrame = {
    val df = session.read.format(format) // define file format
      .option("header", header) // use header line for column names
      .option("sep", separator) // define column separator
      .option("timestampFormat", timestampFormat) // define date format
      .load(path) // load file form path into DataFrame

    // print debugging output
    logger.info("read {} lines from: {}", df.count(), path)

    // return DataFrame
    df
  }
}

object TranslogReader {

  def from(folder: String): TranslogReader = {
    new TranslogReader(folder)
  }
}

class FileExtensionfilter(fileExtension: String) extends FilenameFilter {

  override def accept(dir: File, name: String): Boolean = {
    dir.getName.endsWith("." + fileExtension)
  }
}