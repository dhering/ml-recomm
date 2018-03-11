package de.dhrng.ml.recomm.transformer

import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.mllib.fpm.AssociationRules
import org.apache.spark.mllib.fpm.FPGrowth.FreqItemset
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

class TransitionProbabilitiesTransformer(sparkSession: SparkSession, minConfidence: Double) extends Transformer {

  override val uid: String = ""

  override def transform(translogs: Dataset[_]): DataFrame = {
    
    val freqItemsets = translogs
        .toDF().rdd
        .map(row => new FreqItemset[String](
          Array(row.getAs("antecedent"), row.getAs("consequent")), Integer.toUnsignedLong(row.getAs("frequency"))
        ))

    val collect = freqItemsets.collect

    val associationRules = new AssociationRules()
    associationRules.setMinConfidence(minConfidence)

    // TODO: why has the result zero entries???
    val rules = associationRules.run[String](freqItemsets)
      .map(rule => (rule.antecedent(0), rule.consequent(0), rule.confidence))

    sparkSession.createDataFrame(rules)//.select("antecedent", "consequent", "confidence")
  }

  override def copy(extra: ParamMap): Transformer = {
    copyValues(new TransitionProbabilitiesTransformer(sparkSession, minConfidence), extra)
  }

  override def transformSchema(schema: StructType): StructType = {
    StructType(
      StructField("antecedent", StringType, false) ::
        StructField("consequent", StringType, false) ::
        StructField("confidence", DoubleType, false) :: Nil)
  }

}
