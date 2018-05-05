package de.dhrng.ml.recomm.agent

import de.dhrng.ml.recomm.model.ml.{ActionValue, ActionValuesByState}

import scala.util.Random

class Agent(model: Map[String, ActionValuesByState]) {

  val stateList: List[String] = model.keySet.toList
  val policy = new EGreedyPolicy(stateList)

  def predict(state: String, amount: Int): Seq[String] = {

    val premiseState = model.get(state)

    if (premiseState.isEmpty) {
      return Seq()
    }

    policy.select(premiseState.get.actionValues, amount)
  }
}
