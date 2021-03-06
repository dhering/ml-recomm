package de.dhrng.ml.recomm.transformer

import de.dhrng.ml.recomm.common.ColumnDefinition._
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

class TransitionProbabilitiesTransformer(sparkSession: SparkSession) extends Transformer {

  override val uid: String = getClass.getName.hashCode.toString

  val COL_ANTECEDENT_FREQUENCY = StructField("antecedent_frequency", LongType, nullable = false)

  val ANTECEDENT: String = COL_ANTECEDENT.name
  val CONSEQUENT: String = COL_CONSEQUENT.name
  val FREQUENCY: String = COL_FREQUENCY.name
  val PROBABILITY: String = COL_PROBABILITY.name

  val ANTECEDENT_FREQUENCY: String = COL_ANTECEDENT_FREQUENCY.name

  override def transform(dataset: Dataset[_]): DataFrame = {

    import sparkSession.implicits._

    // cache for DAG optimization
    val cachedDataset = dataset.cache()

    // create FreqItemset of (antecedent, frequency)
    val antecedentFreq = createAntecedentFreq(cachedDataset)

    // join datasets by 'antecedent' column
    val frequencies = cachedDataset.join(antecedentFreq, ANTECEDENT)

    // select columns and calculate probability
    frequencies.select($"$ANTECEDENT", $"$CONSEQUENT", $"$FREQUENCY" / $"$ANTECEDENT_FREQUENCY" as PROBABILITY)
  }

  def createAntecedentFreq(dataset: Dataset[_]): DataFrame = {

    dataset.toDF()
      // select only antecedent items and sum frequencies of equal item IDs
      .select(ANTECEDENT, FREQUENCY)
      .groupBy(ANTECEDENT)
      .sum(FREQUENCY)
      // rename generated column 'sum(FREQUENCY)' to 'ANTECEDENT_FREQUENCY'
      .withColumnRenamed("sum(FREQUENCY)", ANTECEDENT_FREQUENCY)
  }

  override def copy(extra: ParamMap): Transformer = {
    copyValues(new TransitionProbabilitiesTransformer(sparkSession), extra)
  }

  override def transformSchema(schema: StructType): StructType = {
    StructType(Seq(COL_ANTECEDENT, COL_CONCLUSION, COL_PROBABILITY))
  }
}
