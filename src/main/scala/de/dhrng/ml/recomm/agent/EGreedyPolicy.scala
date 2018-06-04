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
      .map(state =>
        if (explore()) randomState(conclusionStates).getOrElse(state)
        else state
      )
  }

  def explore(): Boolean = {

    if (epsilon <= 0.0) {
      return false
    } else if (epsilon >= 1.0){
      return true
    }

    Random.nextDouble() <= epsilon
  }

  def randomState(usedStates: Seq[String], retry: Int = 10): Option[String] = {

    if(retry <= 0){
      // limit retry recursion
      return None
    }

    val result = stateList(Random.nextInt(stateList.size))

    if (usedStates.contains(result)) {
      // search next random state, if random value is already used
      return randomState(usedStates, retry -1)
    }

    Some(result)
  }
}
