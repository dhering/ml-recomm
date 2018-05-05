package de.dhrng.ml.recomm.policy

import de.dhrng.ml.recomm.model.ml.ActionValue

trait Policy extends Serializable{
  def select(probabilities: Seq[ActionValue]): ActionValue
}
