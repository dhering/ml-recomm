package de.dhrng.ml.recomm.transformer

import de.dhrng.ml.recomm.common.ColumnDefinition._
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

class FrequentItemSetTransformer(sparkSession: SparkSession, markEnding: Boolean = false) extends Transformer {

  override val uid: String = getClass.getName.hashCode.toString

  // shortcuts for column names
  val TRANSACTION_ID: String = COL_TRANSACTION_ID.name
  val ITEM_ID: String = COL_ITEM_ID.name

  val ANTECEDENT: String = COL_ANTECEDENT.name
  val CONSEQUENT: String = COL_CONSEQUENT.name
  val FREQUENCY: String = COL_FREQUENCY.name

  override def transform(translogs: Dataset[_]): DataFrame = {
    // register implicits for spark session
    import sparkSession.implicits._

    val freqItemSets = translogs.toDF()
      // group by first column (transaction ID)
      .groupByKey(_.getAs[String](TRANSACTION_ID))

      // collect transitions by grouped transactions
      .flatMapGroups((_, rows) => mapToTransitions(rows))

      // allow only pairs of different itemIDs
      .filter(pair => pair._1 != pair._2)

      // group by (antecedent, consequent) and count as (frequency)
      .select('_1 as ANTECEDENT, '_2 as CONSEQUENT)
      .groupBy(ANTECEDENT, CONSEQUENT)
      .count().withColumnRenamed("count", FREQUENCY)

    freqItemSets
  }

  private def mapToTransitions(rows: Iterator[Row]):
    TraversableOnce[(String, String)] = {

    var result = Seq.empty[(String, String)]

    var lastItemID: String = null

    for (row <- rows) {
      val itemId = row.getAs[String](ITEM_ID)

      // Ignore next line, if there is no last item. This case can only be the first item in list
      if (lastItemID != null) {
        // add a mapping between the last item and the current item with a count of one
        result = result :+ (lastItemID, itemId)
      }
      // set current item to last item for the next loop
      lastItemID = itemId
    }

    // mark last item of a session
    if (markEnding) {
      result = result :+ (lastItemID, "#END#")
    }

    result
  }

  override def copy(extra: ParamMap): Transformer = {
    copyValues(new FrequentItemSetTransformer(sparkSession), extra)
  }

  override def transformSchema(schema: StructType): StructType = {
    StructType(Seq(COL_ANTECEDENT, COL_CONSEQUENT, COL_FREQUENCY))
  }
}
