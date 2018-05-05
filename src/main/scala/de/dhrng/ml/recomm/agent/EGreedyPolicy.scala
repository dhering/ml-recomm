package de.dhrng.ml.recomm.agent

import de.dhrng.ml.recomm.model.ml.ActionValue

import scala.util.Random

class EGreedyPolicy(stateList: List[String], var epsilon: Double = 0.1) {

  def select(actionValues: Seq[ActionValue], amount: Int): Seq[String] = {

    // select conclusion states greedy
    val conclusionStates = actionValues
      .sortWith(_.value > _.value)
      .take(amount)
      .map(_.state)

    // explore states with a randomness of epsilon
    conclusionStates
      .map(state => if (explore()) randomState(conclusionStates) else state)
  }

  def randomState(usedStates: Seq[String]): String = {

    var result = stateList(Random.nextInt(stateList.size))

    if (usedStates.contains(result)) {
      // search next random state, if random value is already used
      result = randomState(usedStates)
    }

    result
  }

  def explore(): Boolean = {

    if (epsilon == 0) {
      return false
    }

    Random.nextDouble() <= epsilon
  }
}
