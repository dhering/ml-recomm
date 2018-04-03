package de.dhrng.ml.recomm.estimator

import de.dhrng.ml.recomm.estimator.ActionValueFunctionEstimator.{ProbabilitiesByState, Probability}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.ml.Estimator
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.types.StructType

class ActionValueFunctionEstimator(val session: SparkSession, val gamma: Double = 0.5, val episodeEndingDepth: Int = 10) extends Estimator[ActionValueModel] {

  import ActionValueModel._

  // Members declared in org.apache.spark.ml.util.Identifiable
  val uid: String = ""
  var pricing: Map[String, Double] = Map()

  def setPricing(pricing: Map[String, Double]): ActionValueFunctionEstimator = {
    this.pricing = pricing

    this
  }

  // Members declared in org.apache.spark.ml.Estimator
  override def copy(extra: org.apache.spark.ml.param.ParamMap): ActionValueFunctionEstimator = {
    copyValues(new ActionValueFunctionEstimator(session), extra)
  }

  def fit(dataset: Dataset[_]): ActionValueModel = {

    val ANTECEDENT = dataset.schema.fields(0).name

    // cache for DAG optimization
    val cachedDataset = dataset.cache()

    val transitionProbabilities = createTransitionProbabilities(cachedDataset)
    val pricing = session.sparkContext.broadcast(this.pricing)

    val actionValues = cachedDataset.toDF().rdd
      .map(row => {
        val antecedent = row.getString(0)
        val consequent = row.getString(1)
        val confidence = row.getDouble(2)

        val actionValue = confidence + calculateActionValue(consequent, transitionProbabilities.value, pricing.value, 2)

        Row(antecedent, consequent, actionValue)
      })
      .cache()


    new ActionValueModel(session.createDataFrame(actionValues, transformSchema(dataset.schema)))
  }

  /**
    * map (key, (antecedent, consequent, confidence)) to {@link ProbabilitiesByState}
    *
    * @param group a tuple of (key, (antecedent, consequent, confidence))
    * @return new ProbabilitiesByState object
    */
  def mapTo(group: (String, Iterable[(String, String, Double)])): ProbabilitiesByState = {

    val key = group._1
    val rows = group._2

    val probabilities = rows.map(row => Probability(row._3, row._2)).toList

    ProbabilitiesByState(key, probabilities)
  }

  def createTransitionProbabilities(cachedDataset: Dataset[_]): Broadcast[Map[String, ProbabilitiesByState]] = {

    def probabilitiesByStates = cachedDataset.toDF().rdd
      .map(row => (row.getString(0), row.getString(1), row.getDouble(2)))
      .groupBy(_._1)
      .map(group => {
        val probabilities = group._2.map(row => Probability(row._3, row._2)).toList

        (group._1, ProbabilitiesByState(group._1, probabilities))
      })
      .collect().toMap[String, ProbabilitiesByState]

    session.sparkContext.broadcast(probabilitiesByStates)
  }

  def calculateActionValue(state: String, transitionPropabilities: Map[String, ProbabilitiesByState], pricing: Map[String, Double], depth: Int): Double = {

    if (depth > episodeEndingDepth) {
      return 0 // end episode
    }

    val stateProbabilities = transitionPropabilities.get(state)

    if (stateProbabilities.isEmpty) {
      return 0 // end if state probabilities are empty
    }

    val probabilities = stateProbabilities.get.probabilities

    // select next state by highest probability
    val nextStateProbability = probabilities.sortWith(_.probability > _.probability).head

    val price = pricing.getOrElse(nextStateProbability.state, 0D)

    return scala.math.pow(gamma, depth) * price * nextStateProbability.probability +
      calculateActionValue(nextStateProbability.state, transitionPropabilities, pricing, depth + 1)
  }

  def transformSchema(schema: StructType): StructType = {
    StructType(Seq(COL_PREMISE, COL_CONCLUSION, COL_ACTION_VALUE))
  }
}

object ActionValueFunctionEstimator {

  case class ProbabilitiesByState(state: String, probabilities: Seq[Probability]) extends Serializable

  case class Probability(probability: Double, state: String) extends Serializable {

    override def toString: String = state + ": " + probability
  }

}