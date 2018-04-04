package de.dhrng.ml.recomm.estimator

import de.dhrng.ml.recomm.common.ColumnDefinition._
import org.apache.spark.ml.Model
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset}

object ActionValueModel {
  val COL_ACTION_VALUE = StructField("value", DoubleType, nullable = false)
}

class ActionValueModel(val m: DataFrame) extends Model[ActionValueModel] {

  import ActionValueModel._

  override val uid: String = ""

  val model = m

  /**
    * Create a copy of the model.
    * The copy is shallow, except for the embedded paramMap, which gets a deep copy.
    *
    * This is used for the default implementation of [[transform()]].
    */
  override def copy(extra: ParamMap): ActionValueModel = {
    copyValues(new ActionValueModel(model), extra).setParent(parent)
  }

  override def transform(dataset: Dataset[_]): DataFrame = ???

  override def transformSchema(schema: StructType): StructType = {
    StructType(Seq(COL_PREMISE, COL_CONCLUSION, COL_ACTION_VALUE))
  }
}
