package de.dhrng.ml.recomm.transformer

import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

class FrequentItemSetTransformer(sparkSession: SparkSession) extends Transformer {

  override val uid: String = ""

  override def transform(translogs: Dataset[_]): DataFrame = {

    val freqItemsets = translogs
      .toDF() // keep DF to work with Rows
      .rdd // transform to RDD
      .map(row => (row.getAs("transactID"), row.getAs("itemID")): (String, String)) // reduce data to transaction ID and item ID per line
      .groupByKey() // group by transaction ID
      .flatMap(toTransitions)
      .filter(pair => pair._1 != pair._2) // allow only pairs of different itemIDs
      .map(pair => (pair, 1))
      .reduceByKey((a, b) => a + b)
      .map(row => Row(row._1._1, row._1._2, row._2)) // map Tuple of ((String, String) Long) to a Tuple of 3
    
    sparkSession.createDataFrame(freqItemsets, transformSchema())
  }

  // TODO: write test
  private def toTransitions(group: (String, Iterable[String])): Seq[(String, String)] = {
    var result = Seq.empty[(String, String)]

    var lastItemID: String = null

    for (itemid <- group._2.toList) {
      // Ignore next line, if there is no last item. This case can only be the first item in list
      if (lastItemID != null) {
        // add a mapping between the last item and the current item with a count of one
        result = result :+ (lastItemID, itemid);
      }
      // set current item to last item for the next loop
      lastItemID = itemid;
    }

    // mark last item of a session
    result :+ (lastItemID, "#END#");
  }

  override def copy(extra: ParamMap): Transformer = {
    return copyValues(new FrequentItemSetTransformer(sparkSession), extra)
  }

  def transformSchema(): StructType = {
      transformSchema(new StructType)
  }

  override def transformSchema(schema: StructType): StructType = {
    StructType(
      StructField("antecedent", StringType, false) ::
      StructField("consequent", StringType, false) ::
        StructField("frequency", IntegerType, false) :: Nil)
  }

}
