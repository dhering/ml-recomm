package de.dhrng.ml.recomm.transformer

import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.mllib.fpm.AssociationRules
import org.apache.spark.mllib.fpm.FPGrowth.FreqItemset
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

class TransitionProbabilitiesTransformer(sparkSession: SparkSession, minConfidence: Double) extends Transformer {

  override val uid: String = ""

  val COL_CONFIDENCE = StructField("confidence", DoubleType, false)
  val CONFIDENCE = COL_CONFIDENCE.name

  override def transform(dataset: Dataset[_]): DataFrame = {

    // create FreqItemset of ((antecedent, consequent), frequency)
    val pairFrequencies = createPairFrequencies(dataset)

    // create FreqItemset of ((antecedent), frequency)
    val antecedentFreq = createAntecedentFreq(dataset)

    // union both FreqItemsets
    val frequencies = pairFrequencies.union(antecedentFreq)

    // initialize 
    val associationRules = new AssociationRules()
    associationRules.setMinConfidence(minConfidence)

    val rules = associationRules.run[String](frequencies)
      .distinct()
      .map(rule => Row(rule.antecedent(0), rule.consequent(0), rule.confidence))

    sparkSession.createDataFrame(rules, transformSchema(dataset.schema))
  }

  def createPairFrequencies(dataset: Dataset[_]) = {

    val ANTECEDENT = dataset.schema.fields(0).name;
    val CONSEQUENT = dataset.schema.fields(1).name;
    val FREQUENCY = dataset.schema.fields(2).name;

    dataset.toDF().rdd
      // map to FreqItemsets
      .map(row => new FreqItemset[String](
          Array(row.getAs[String](ANTECEDENT), row.getAs[String](CONSEQUENT)),
          row.getAs[Long](FREQUENCY))
      )
  }

  def createAntecedentFreq(dataset: Dataset[_]) = {

    val ANTECEDENT = dataset.schema.fields(0).name;
    val FREQUENCY = dataset.schema.fields(2).name;

    dataset.toDF()
      // select only antecedent items and sum frequencies of equal item IDs
      .select(ANTECEDENT, FREQUENCY).groupBy(ANTECEDENT).sum(FREQUENCY)

      .rdd
      // map to FreqItemsets
      .map(row => new FreqItemset[String](
        Array(row.getAs[String](ANTECEDENT)),
        row.getAs[Long](s"sum($FREQUENCY)"))
      )
  }

  override def copy(extra: ParamMap): Transformer = {
    copyValues(new TransitionProbabilitiesTransformer(sparkSession, minConfidence), extra)
  }

  override def transformSchema(schema: StructType): StructType = {
    StructType(Seq(schema.fields(0), schema.fields(1), COL_CONFIDENCE))
  }

}
