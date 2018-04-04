package de.dhrng.ml.recomm.transformer

import de.dhrng.ml.recomm.common.ColumnDefinition._
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

class TransitionProbabilitiesTransformer(sparkSession: SparkSession) extends Transformer {

  override val uid: String = ""

  val COL_ANTECEDENT_FREQUENCY = StructField("antecedent_frequency", LongType, nullable = false)

  val ANTECEDENT = COL_ANTECEDENT.name
  val CONSEQUENT = COL_CONSEQUENT.name
  val FREQUENCY = COL_FREQUENCY.name
  val PROBABILITY = COL_PROBABILITY.name

  val ANTECEDENT_FREQUENCY = COL_ANTECEDENT_FREQUENCY.name

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

    val freq = dataset.toDF()
      // select only antecedent items and sum frequencies of equal item IDs
      .select(ANTECEDENT, FREQUENCY)
      .rdd
        .map(row => (row.getAs[String](ANTECEDENT), row.getAs[Long](FREQUENCY)))
        .reduceByKey(_ + _)
      .map(row => Row(row._1, row._2))

    sparkSession.createDataFrame(freq, StructType(Seq(COL_ANTECEDENT, COL_ANTECEDENT_FREQUENCY)))
  }

  override def copy(extra: ParamMap): Transformer = {
    copyValues(new TransitionProbabilitiesTransformer(sparkSession), extra)
  }

  override def transformSchema(schema: StructType): StructType = {
    StructType(Seq(COL_ANTECEDENT, COL_CONCLUSION, COL_PROBABILITY))
  }
}
