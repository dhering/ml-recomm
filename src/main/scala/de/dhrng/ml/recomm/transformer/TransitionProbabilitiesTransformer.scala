package de.dhrng.ml.recomm.transformer

import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.mllib.fpm.AssociationRules
import org.apache.spark.mllib.fpm.FPGrowth.FreqItemset
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

class TransitionProbabilitiesTransformer(sparkSession: SparkSession, minConfidence: Double) extends Transformer {

  override val uid: String = ""

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

    val antecedentCol = dataset.schema.fields(0).name;
    val consequentCol = dataset.schema.fields(1).name;
    val frequencyCol = dataset.schema.fields(2).name;


    dataset.toDF().rdd
      .map(row =>
        new FreqItemset[String](
          Array(row.getAs(antecedentCol), row.getAs(consequentCol)),
          Integer.toUnsignedLong(row.getAs(frequencyCol))
        ))
  }

  def createAntecedentFreq(dataset: Dataset[_]) = {

    val antecedentCol = dataset.schema.fields(0).name;
    val frequencyCol = dataset.schema.fields(2).name;

    dataset.toDF().rdd
      .map(row => {
        (row.getAs(antecedentCol), Integer.toUnsignedLong(row.getAs(frequencyCol)))
      }: (String, Long))
      .reduceByKey((a, b) => a + b)
      .map(row => new FreqItemset[String](Array(row._1), row._2))
  }

  override def copy(extra: ParamMap): Transformer = {
    copyValues(new TransitionProbabilitiesTransformer(sparkSession, minConfidence), extra)
  }

  override def transformSchema(schema: StructType): StructType = {
    StructType(
      schema.fields(0) ::
        schema.fields(1) ::
        StructField("confidence", DoubleType, false) :: Nil)
  }

}
