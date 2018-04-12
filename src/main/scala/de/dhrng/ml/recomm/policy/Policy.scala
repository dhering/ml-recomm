package de.dhrng.ml.recomm.policy

import de.dhrng.ml.recomm.model.ml.StateProbability

trait Policy extends Serializable{
  def select(probabilities: Seq[StateProbability]): StateProbability
}
