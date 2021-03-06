package de.dhrng.ml.recomm.persistence

import java.io.BufferedWriter

import org.apache.spark.sql.DataFrame
import org.apache.commons.csv.CSVFormat
import org.apache.commons.csv.CSVPrinter
import java.nio.file.Files
import java.nio.file.Paths

import org.apache.spark.ml.PipelineModel

object CsvWriter {

  def write(model: PipelineModel, path: String): Unit = {

    // get last stage of the trained PipelineModel as ActionValueModel
    val actionValueModel = model.stages.last
      .asInstanceOf[de.dhrng.ml.recomm.estimator.ActionValueModel]

    write(actionValueModel.model, path)
  }

  def write(df: DataFrame, path: String): Unit = {
    // get header/column names from dataframe
    val header = df.schema.fields.map(structField => structField.name)

    var writer: BufferedWriter = null
    var csvPrinter: CSVPrinter = null

    try {
      writer = Files.newBufferedWriter(Paths.get(path))
      csvPrinter = new CSVPrinter(writer, CSVFormat.DEFAULT.withHeader(header: _*))

      val rows = df.collect()

      rows.foreach(row => {
        val fields = row.toSeq.map {
          case field: Double => field: java.lang.Double
          case field: String => field
        }

        csvPrinter.printRecord(fields: _*)

        csvPrinter.flush()
      })
    } finally {
      if (writer != null) writer.close()
      if (csvPrinter != null) csvPrinter.close()
    }
  }
}
