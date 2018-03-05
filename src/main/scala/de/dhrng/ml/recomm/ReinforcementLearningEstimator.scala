package de.dhrng.ml.recomm

import org.apache.spark.SparkContext
import org.apache.spark.ml.Estimator
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.types.StructType

class ReinforcementLearningEstimator(override val uid: String, val sc: SparkContext) extends Estimator[ReinforcementLearningModel]
{

  override def fit(dataset: Dataset[_]): ReinforcementLearningModel = {

    val probabilitiesByState =
      sc.broadcast(transformAssociationRules(dataset.select("state", "nextState", "probability")))


    return new ReinforcementLearningModel(s"${uid}_model");
  }

  override def copy(extra: ParamMap): Estimator[ReinforcementLearningModel] = {
    copyValues(new ReinforcementLearningEstimator(uid, sc), extra)
  }

  override def transformSchema(schema: StructType): StructType = ???


  /**
    * Transform the given {@link DataFrame} of flat association rules into the data model of {@link StateProbabilities}
    * and store them into a map
    *
    * @param df
    * @return map of StateProbabilities
    */
  def transformAssociationRules(df: DataFrame): collection.Map[String, StateProbabilities] = {
    return df.rdd
      .map(row => (row.getString(0), row.getDouble(2), row.getString(1)))
      .groupBy(row => row._1)
      .map(result => mapTo(result._1, result._2))
      .collectAsMap()
  }

  def mapTo(state: String, groupedValues: Iterable[(String, Double, String)]): (String, StateProbabilities) = {
    return (state, new StateProbabilities(state, groupedValues.map(group => new NextStateProbability(group._2, group._3))))
  }

  class StateProbabilities(val state: String, val nextStates: Iterable[NextStateProbability])
  class NextStateProbability(val probability: Double, val state: String)
}
