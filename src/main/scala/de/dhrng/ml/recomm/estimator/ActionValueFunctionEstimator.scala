package de.dhrng.ml.recomm.estimator

import de.dhrng.ml.recomm.common.ColumnDefinition._
import de.dhrng.ml.recomm.model.ml.{ActionValue, ProbabilitiesByState, StateProbability}
import de.dhrng.ml.recomm.policy.GreedyPolicy
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.ml.Estimator
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Dataset, Row, SparkSession}

class ActionValueFunctionEstimator(val session: SparkSession,
                                   val gamma: Double = 0.5,
                                   val episodeEndingDepth: Int = 10,
                                   val minReward: Double = 1.0) extends Estimator[ActionValueModel] {

  import ActionValueModel._

  override val uid: String = getClass.getName.hashCode.toString

  val ANTECEDENT: String = COL_ANTECEDENT.name
  val CONSEQUENT: String = COL_CONSEQUENT.name

  val PROBABILITY: String = COL_PROBABILITY.name

  val policy: GreedyPolicy.type = GreedyPolicy
  var rewards: Map[String, Double] = Map()

  var cachedProbabilitiesByStates: Broadcast[Map[String, ProbabilitiesByState]] = _
  var cachedRewards: Broadcast[Map[String, Double]] = _

  def setRewards(rewards: Map[String, Double]): ActionValueFunctionEstimator = {
    this.rewards = rewards

    this
  }

  // Members declared in org.apache.spark.ml.Estimator
  override def copy(extra: org.apache.spark.ml.param.ParamMap): ActionValueFunctionEstimator = {
    copyValues(new ActionValueFunctionEstimator(session), extra)
  }

  def fit(dataset: Dataset[_]): ActionValueModel = {

    broadcastRewards()

    // cache for DAG optimization
    val cachedDataset = dataset.cache()

    createTransitionProbabilities(cachedDataset)

    val actionValues = cachedDataset.toDF().rdd
      .map(row => {
        val antecedent = row.getAs[String](ANTECEDENT)
        val consequent = row.getAs[String](CONSEQUENT)
        val probability = row.getAs[Double](PROBABILITY)

        val actionValue = calculateActionValue(consequent, probability, 0)

        Row(antecedent, consequent, actionValue)
      })

    new ActionValueModel(session.createDataFrame(actionValues, transformSchema(dataset.schema)))
  }


  /**
    * map (key, (antecedent, consequent, probability)) to `ProbabilitiesByState`
    *
    * @param group a tuple of (key, (antecedent, consequent, probability))
    * @return new ProbabilitiesByState object
    */
  def mapTo(group: (String, Iterable[(String, String, Double)])): ProbabilitiesByState = {

    val key = group._1
    val rows = group._2

    val probabilities = rows.map(row => StateProbability(row._2, row._3)).toList

    ProbabilitiesByState(key, probabilities)
  }

  def broadcastRewards() {
    cachedRewards = session.sparkContext.broadcast(this.rewards)
  }

  def getReward(state: String): Double = {
    cachedRewards.value.getOrElse(state, minReward)
  }

  def createTransitionProbabilities(dataset: Dataset[_]): Unit = {

    val probabilitiesByState = dataset.toDF().rdd
      .map(row => (row.getString(0), row.getString(1), row.getDouble(2)))
      .groupBy(_._1)
      .map(group => {
        val probabilities = group._2.map(row => StateProbability(row._2, row._3)).toList

        (group._1, ProbabilitiesByState(group._1, probabilities))
      })
      .collect().toMap[String, ProbabilitiesByState]

    cachedProbabilitiesByStates = session.sparkContext.broadcast(probabilitiesByState)
  }

  def getProbabilitiesByState(state: String): Option[ProbabilitiesByState] = {
    return cachedProbabilitiesByStates.value.get(state)
  }

  def calcDiscount(depth: Int): Double = scala.math.pow(this.gamma, depth)

  def calculateActionValue(state: String, probability: Double, depth: Int): Double = {

    if (depth >= episodeEndingDepth) {
      return 0 // end episode because of max depth
    }

    // calculate action-value for the current state
    val discount = calcDiscount(depth)
    val reward = getReward(state)

    var actionValue: Double = probability * discount * reward

    // get the following action-values from the MDP
    val stateProbabilities = getProbabilitiesByState(state)

    if (stateProbabilities.nonEmpty) {
      val actionValues = stateProbabilities.get.probabilities
        .map(stateProbability => {
          // calculate action-value for the next depth
          val actionValue = calculateActionValue(stateProbability.state, stateProbability.probability, depth + 1)
          // return action-value als StateProbability object, related to given state
          ActionValue(stateProbability.state, actionValue)
        })

      // select next state by highest probability
      val nextState = policy.select(actionValues)

      actionValue += nextState.value
    }

    return actionValue
  }

  def transformSchema(schema: StructType): StructType = {
    StructType(Seq(COL_PREMISE, COL_CONCLUSION, COL_ACTION_VALUE))
  }
}