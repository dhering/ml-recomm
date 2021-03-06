package de.dhrng.ml.recomm.transformer

import de.dhrng.ml.recomm.common.ColumnDefinition._
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

class FilterTransformer(sparkSession: SparkSession) extends Transformer {

  override val uid: String = getClass.getName.hashCode.toString

  // shortcuts for column names
  val TRANSACTION_ID: String = COL_TRANSACTION_ID.name
  val ITEM_ID: String = COL_ITEM_ID.name
  val TRAN_TYPE: String = "transType"

  override def transform(translogs: Dataset[_]): DataFrame = {
    translogs
      .select(TRANSACTION_ID, ITEM_ID, TRAN_TYPE)
      .filter (row => !row.getAs[String](ITEM_ID).startsWith("__"))
      .filter (row => row.getAs[String](TRAN_TYPE) == "-1")
      .select(TRANSACTION_ID, ITEM_ID)
  }

  override def copy(extra: ParamMap): Transformer = {
    copyValues(new FilterTransformer(sparkSession), extra)
  }

  override def transformSchema(schema: StructType): StructType = {
    StructType(Seq(COL_TRANSACTION_ID, COL_ITEM_ID))
  }

}
