package de.dhrng.ml.recomm.writer

import org.apache.spark.sql.DataFrame
import org.apache.commons.csv.CSVFormat
import org.apache.commons.csv.CSVPrinter
import java.nio.file.Files
import java.nio.file.Paths

object CsvWriter {

  def write(df: DataFrame, path: String): Unit = {
    // get header/column names from dataframe
    val header = df.schema.fields.map(structField => structField.name)

    try {
      val writer = Files.newBufferedWriter(Paths.get(path))
      val csvPrinter = new CSVPrinter(writer, CSVFormat.DEFAULT.withHeader(header: _*))
      try {

        val rows = df.collect()

        rows.foreach(row => {
          val fields = row.toSeq.map{
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
}
