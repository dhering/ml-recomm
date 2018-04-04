package de.dhrng.ml.recomm.common

import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField}

object ColumnDefinition {

  val COL_TRANSACTION_ID = StructField("transactID", StringType, nullable = false)
  val COL_ITEM_ID = StructField("itemID", StringType, nullable = false)

  val COL_ANTECEDENT = StructField("antecedent", StringType, nullable = false)
  val COL_CONSEQUENT = StructField("consequent", StringType, nullable = false)
  val COL_FREQUENCY = StructField("frequency", IntegerType, nullable = false)

  val COL_PREMISE = StructField("premise", StringType, nullable = false)
  val COL_CONCLUSION = StructField("conclusion", StringType, nullable = false)
  val COL_PROBABILITY = StructField("probability", DoubleType, nullable = false)
}
