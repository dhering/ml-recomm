package de.dhrng.ml.recomm.transformer

import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset}

class FrequentItemSetTransformer extends Transformer {

  override val uid: String = ""

  override def transform(translogs: Dataset[_]): DataFrame = {

    return translogs.filter(_ != null).toDF()
  }

  override def copy(extra: ParamMap): Transformer = {
    return copyValues(new FrequentItemSetTransformer(), extra)
  }

  override def transformSchema(schema: StructType): StructType = ???

}
