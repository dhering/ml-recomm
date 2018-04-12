package de.dhrng.ml.recomm.policy

import de.dhrng.ml.recomm.model.ml.StateProbability

object GreedyPolicy extends Policy{

  def select(probabilities: Seq[StateProbability]): StateProbability = {

    probabilities.sortWith(_.probability > _.probability).head
  }
}
