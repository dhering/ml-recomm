package de.dhrng.ml.recomm.estimator

import org.apache.spark.ml.Model
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset}

class ReinforcementLearningModel(override val uid: String) extends Model[ReinforcementLearningModel] {

  /**
    * Create a copy of the model.
    * The copy is shallow, except for the embedded paramMap, which gets a deep copy.
    *
    * This is used for the default implementation of [[transform()]].
    */
  override def copy(extra: ParamMap): ReinforcementLearningModel = {
    copyValues(new ReinforcementLearningModel(uid), extra).setParent(parent)
  }

  override def transform(dataset: Dataset[_]): DataFrame = ???

  override def transformSchema(schema: StructType): StructType = {
    StructType(
      StructField("premise", StringType, true) ::
        StructField("conclusion", StringType, false) ::
        StructField("value", DoubleType, false) :: Nil)
  }
}
