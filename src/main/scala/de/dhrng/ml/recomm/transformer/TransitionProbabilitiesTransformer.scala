package de.dhrng.ml.recomm.transformer

import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

class TransitionProbabilitiesTransformer(sparkSession: SparkSession, minConfidence: Double) extends Transformer {

  override val uid: String = ""

  val COL_ANTECEDENT_FREQUENCY = StructField("antecedent_frequency", LongType, nullable = false)
  val COL_CONFIDENCE = StructField("confidence", DoubleType, nullable = false)
  val CONFIDENCE: String = COL_CONFIDENCE.name

  override def transform(dataset: Dataset[_]): DataFrame = {

    import sparkSession.implicits._

    // get column names from dataset
    val ANTECEDENT = getAntecedentColumn(dataset).name
    val CONSEQUENT = getConsequentColumn(dataset).name
    val FREQUENCY = getFrequencyColumn(dataset).name
    val ANTECEDENT_FREQUENCY = COL_ANTECEDENT_FREQUENCY.name

    // cache for DAG optimization
    val cachedDataset = dataset.cache()

    // create FreqItemset of (antecedent, frequency)
    val antecedentFreq = createAntecedentFreq(cachedDataset)

    // join datasets by 'antecedent' column
    val frequencies = cachedDataset.join(antecedentFreq, ANTECEDENT)

    // select columns and calculate confidence
    frequencies.select($"$ANTECEDENT", $"$CONSEQUENT", $"$FREQUENCY" / $"$ANTECEDENT_FREQUENCY" as CONFIDENCE)
  }

  def createAntecedentFreq(dataset: Dataset[_]): DataFrame = {

    val COL_ANTECEDENT = getAntecedentColumn(dataset)
    val ANTECEDENT = COL_ANTECEDENT.name
    val FREQUENCY = getFrequencyColumn(dataset).name

    val freq = dataset.toDF()
      // select only antecedent items and sum frequencies of equal item IDs
      .select(ANTECEDENT, FREQUENCY)
      .rdd
        .map(row => (row.getAs[String](ANTECEDENT), row.getAs[Long](FREQUENCY)))
        .reduceByKey(_ + _)
      .map(row => Row(row._1, row._2))

    sparkSession.createDataFrame(freq, StructType(Seq(COL_ANTECEDENT, COL_ANTECEDENT_FREQUENCY)))
  }

  private def getAntecedentColumn(dataset: Dataset[_]): StructField = {
    dataset.schema.fields(0)
  }

  private def getConsequentColumn(dataset: Dataset[_]): StructField = {
    dataset.schema.fields(1)
  }

  private def getFrequencyColumn(dataset: Dataset[_]): StructField = {
    dataset.schema.fields(2)
  }

  override def copy(extra: ParamMap): Transformer = {
    copyValues(new TransitionProbabilitiesTransformer(sparkSession, minConfidence), extra)
  }

  override def transformSchema(schema: StructType): StructType = {
    StructType(Seq(schema.fields(0), schema.fields(1), COL_CONFIDENCE))
  }

}
