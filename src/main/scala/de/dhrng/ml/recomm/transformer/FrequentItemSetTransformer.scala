package de.dhrng.ml.recomm.transformer

import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

class FrequentItemSetTransformer(sparkSession: SparkSession) extends Transformer {

  override val uid: String = ""

  override def transform(translogs: Dataset[_]): DataFrame = {
    val transactIdCol = translogs.schema.fields(0).name
    val itemIdCol = translogs.schema.fields(1).name

    val freqItemSets = translogs
      .toDF() // keep DF to work with Rows
      .rdd // transform to RDD
      .map(row => (row.getAs(transactIdCol), row.getAs(itemIdCol)): (String, String)) // reduce data to transaction ID and item ID per line
      .groupByKey() // group by transaction ID
      .flatMap(toTransitions)
      .filter(pair => pair._1 != pair._2) // allow only pairs of different itemIDs
      .map(pair => (pair, 1))
      .reduceByKey((a, b) => a + b)
      .map(row => Row(row._1._1, row._1._2, row._2)) // map Tuple of ((String, String) Long) to a Tuple of 3

    sparkSession.createDataFrame(freqItemSets, transformSchema(translogs.schema))
  }

  // TODO: write test
  private def toTransitions(group: (String, Iterable[String])): Seq[(String, String)] = {
    var result = Seq.empty[(String, String)]

    val itemIds = group._2.toList;
    var lastItemID: String = null

    for (itemId <- itemIds) {
      // Ignore next line, if there is no last item. This case can only be the first item in list
      if (lastItemID != null) {
        // add a mapping between the last item and the current item with a count of one
        result = result :+ (lastItemID, itemId);
      }
      // set current item to last item for the next loop
      lastItemID = itemId;
    }

    // mark last item of a session
    result :+ (lastItemID, "#END#");
  }

  override def copy(extra: ParamMap): Transformer = {
    return copyValues(new FrequentItemSetTransformer(sparkSession), extra)
  }

  override def transformSchema(schema: StructType): StructType = {
    StructType(
      StructField("antecedent", StringType, false) ::
        StructField("consequent", StringType, false) ::
        StructField("frequency", IntegerType, false) :: Nil)
  }

}
