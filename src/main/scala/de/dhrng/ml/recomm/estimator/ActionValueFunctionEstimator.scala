package de.dhrng.ml.recomm.estimator

import de.dhrng.ml.recomm.common.ColumnDefinition._
import de.dhrng.ml.recomm.estimator.ActionValueFunctionEstimator.{ProbabilitiesByState, Probability}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.ml.Estimator
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Dataset, Row, SparkSession}

class ActionValueFunctionEstimator(val session: SparkSession, val gamma: Double = 0.5, val episodeEndingDepth: Int = 10, val minPrice: Double = 0.01D) extends Estimator[ActionValueModel] {

  import ActionValueModel._

  override val uid: String = getClass.getName.hashCode.toString

  val ANTECEDENT: String = COL_ANTECEDENT.name
  val CONSEQUENT: String = COL_CONSEQUENT.name

  val PROBABILITY: String = COL_PROBABILITY.name
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

    // cache for DAG optimization
    val cachedDataset = dataset.cache()

    val transitionProbabilities = createTransitionProbabilities(cachedDataset)
    val pricing = session.sparkContext.broadcast(this.pricing)

    val actionValues = cachedDataset.toDF().rdd
      .map(row => {
        val antecedent = row.getAs[String](ANTECEDENT)
        val consequent = row.getAs[String](CONSEQUENT)
        val probability = row.getAs[Double](PROBABILITY)
        val price = pricing.value.getOrElse(consequent, minPrice)

        val actionValue = probability * price + calculateActionValue(consequent, transitionProbabilities.value, pricing.value, 2)

        Row(antecedent, consequent, actionValue)
      })
      .cache()


    new ActionValueModel(session.createDataFrame(actionValues, transformSchema(dataset.schema)))
  }

  /**
    * map (key, (antecedent, consequent, probability)) to {@link ProbabilitiesByState}
    *
    * @param group a tuple of (key, (antecedent, consequent, probability))
    * @return new ProbabilitiesByState object
    */
  def mapTo(group: (String, Iterable[(String, String, Double)])): ProbabilitiesByState = {

    val key = group._1
    val rows = group._2

    val probabilities = rows.map(row => Probability(row._3, row._2)).toList

    ProbabilitiesByState(key, probabilities)
  }

  def createTransitionProbabilities(cachedDataset: Dataset[_]): Broadcast[Map[String, ProbabilitiesByState]] = {

    val probabilitiesByStates = cachedDataset.toDF().rdd
      .map(row => (row.getString(0), row.getString(1), row.getDouble(2)))
      .groupBy(_._1)
      .map(group => {
        val probabilities = group._2.map(row => Probability(row._3, row._2)).toList

        (group._1, ProbabilitiesByState(group._1, probabilities))
      })
      .collect().toMap[String, ProbabilitiesByState]

    session.sparkContext.broadcast(probabilitiesByStates)
  }

  def calcDiscount(gamma: Double, depth: Int): Double = scala.math.pow(gamma, depth)

  def calculateActionValue(state: String, transitionProbabilities: Map[String, ProbabilitiesByState], pricing: Map[String, Double], depth: Int): Double = {

    if (depth > episodeEndingDepth) {
      return 0 // end episode
    }

    val stateProbabilities = transitionProbabilities.get(state)

    if (stateProbabilities.isEmpty) {
      return 0 // end if state probabilities are empty
    }

    val probabilities = stateProbabilities.get.probabilities

    // select next state by highest probability
    val nextState = probabilities.sortWith(_.probability > _.probability).head

    val discount = calcDiscount(gamma, depth)
    val price = pricing.getOrElse(nextState.state, minPrice)

    return discount * price * nextState.probability +
      calculateActionValue(nextState.state, transitionProbabilities, pricing, depth + 1)
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