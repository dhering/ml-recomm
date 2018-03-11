package de.dhrng.ml.recomm.transformer

import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

class FilterTransformer(sparkSession: SparkSession) extends Transformer {

  override val uid: String = ""

  override def transform(translogs: Dataset[_]): DataFrame = {
    translogs
      .select("transactID", "itemID")
      .filter(row => !row.getAs[String]("itemID").startsWith("__"))
  }

  override def copy(extra: ParamMap): Transformer = {
    copyValues(new FilterTransformer(sparkSession), extra)
  }

  override def transformSchema(schema: StructType): StructType = {
    StructType(
      StructField("transactID", StringType, false) ::
        StructField("itemID", StringType, false) :: Nil)
  }

}
