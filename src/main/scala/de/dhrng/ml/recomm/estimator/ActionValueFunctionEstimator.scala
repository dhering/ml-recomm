package de.dhrng.ml.recomm.estimator

import org.apache.spark.ml.Estimator
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.types.StructType

class ActionValueFunctionEstimator(val session: SparkSession, val gamma: Double = 0.5, val episodeDepth: Int = 10) extends Estimator[ActionValueModel] {

  import ActionValueModel._

  // Members declared in org.apache.spark.ml.util.Identifiable
  val uid: String = ""

  // Members declared in org.apache.spark.ml.Estimator
  override def copy(extra: org.apache.spark.ml.param.ParamMap): ActionValueFunctionEstimator = {
    copyValues(new ActionValueFunctionEstimator(session), extra)
  }

  def fit(dataset: Dataset[_]): ActionValueModel = {

    val ANTECEDENT = dataset.schema.fields(0).name

    // cache for DAG optimization
    val cachedDataset = dataset.cache()

    val transitionPropabilities = createTransitionPropabilities(cachedDataset).value

    val actionValues = cachedDataset.toDF().rdd
      .map(row => {
        val antecedent = row.getString(0)
        val consequent = row.getString(1)
        val confidence = row.getDouble(2)

        val actionValue = confidence + actionValueFunction(consequent, transitionPropabilities, 2)

        Row(antecedent, consequent, actionValue)
      })
      .cache()


    new ActionValueModel(session.createDataFrame(actionValues, transformSchema(dataset.schema)))
  }

  def mapTo(group: (String, Iterable[(String, String, Double)])): ProbabilitiesByState = {
    val probabilities = group._2.map(row => new Probability(row._3, row._2)).toList

    new ProbabilitiesByState(group._1, probabilities)
  }

  def createTransitionPropabilities(cachedDataset: Dataset[_]) = {

    def probabilitiesByState = cachedDataset.toDF().rdd
      .map(row => (row.getString(0), row.getString(1), row.getDouble(2)))
      .groupBy(_._1)
      .map(group => {
        val probabilities = group._2.map(row => new Probability(row._3, row._2)).toList

        (group._1, new ProbabilitiesByState(group._1, probabilities))
      })
      .collectAsMap()

    session.sparkContext.broadcast(probabilitiesByState)
  }

  def actionValueFunction(state: String, transitionPropabilities: collection.Map[String, ProbabilitiesByState], depth: Int): Double = {

    if(depth >= episodeDepth){
      return 0
    }

    val stateProbabilities = transitionPropabilities.get(state)

    if(stateProbabilities.isEmpty){
      return 0
    }

    val probabilities = stateProbabilities.get.probabilities

    val nextStateProbability = probabilities.sortWith(_.probability > _.probability)(0)

    scala.math.pow(gamma, depth) * nextStateProbability.probability +
  actionValueFunction(nextStateProbability.state, transitionPropabilities, depth + 1)
}

// Members declared in org.apache.spark.ml.PipelineStage
  def transformSchema(schema: StructType): StructType = {
    StructType(Seq(COL_PREMISE, COL_CONCLUSION, COL_ACTION_VALUE))
  }

  class ProbabilitiesByState(s: String, p: Seq[Probability]) extends Serializable  {

    val state = s
    val probabilities = p
  }

  class Probability(p: Double, s: String) extends Serializable {

    val probability = p
    val state = s

    override def toString: String = state + ": " + probability
  }

}