package de.dhrng.ml.recomm.writer

import java.io.FileReader

import de.dhrng.ml.recomm.model.ml.{ActionValue, ActionValuesByState}
import org.apache.commons.csv.{CSVFormat, CSVParser, CSVRecord}

import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer

object CsvReader {

  def read(path: String): Map[String, ActionValuesByState] = {

    var in: FileReader = null
    var records: CSVParser = null

    try {
      in = new FileReader(path)
      records = CSVFormat.DEFAULT.withHeader().parse(in)

      return createModel(records.getRecords.toList)

    } finally {
      if (records != null) records.close()
      if (in != null) in.close()
    }

    Map()
  }

  protected def createModel(records: List[CSVRecord]): Map[String, ActionValuesByState] = {

    val rawModel = collectFromRecords(records)

    mapModel(rawModel)
  }

  /**
    * collect action values by state
    *
    * @param records list of CSV records
    * @return a map of premise states with a list of action values
    */
  def collectFromRecords(records: List[CSVRecord]): Map[String, ListBuffer[(String, Double)]] = {

    val model = scala.collection.mutable.Map[String, ListBuffer[(String, Double)]]()

    records.map(record => {

      val premise = record.get("premise")
      val conclusion = record.get("conclusion")
      val value = record.get("value")

      val actionValue = (conclusion, value.toDouble)

      val actionValuesByState = model.get(premise)

      if (actionValuesByState.isEmpty) {
        // add an new entry, if premise is unknown until now
        model.put(premise, ListBuffer(actionValue))
      } else {
        actionValuesByState.get += actionValue
      }
    })

    // return immutable map
    model.toMap
  }

  /**
    * map raw model to real model objects
    *
    * @param rawModel raw model structure
    * @return
    */
  def mapModel(rawModel: Map[String, ListBuffer[(String, Double)]]): Map[String, ActionValuesByState] = {

    rawModel.map {
      case (key: String, value: ListBuffer[(String, Double)]) =>

        val actionValues = value.map {
          // map a Tuple[String, Double] to a ActionValue
          case (state: String, value: Double) => ActionValue(state, value)
        } toList // convert to immutable list

        (key, ActionValuesByState(key, actionValues))
    }
  }
}
