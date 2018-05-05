package de.dhrng.ml.recomm.policy

import de.dhrng.ml.recomm.model.ml.ActionValue

object GreedyPolicy extends Policy {

  def select(probabilities: Seq[ActionValue]): ActionValue = {

    probabilities.sortWith(_.value > _.value).head
  }
}
