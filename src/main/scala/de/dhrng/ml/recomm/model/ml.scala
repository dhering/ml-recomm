package de.dhrng.ml.recomm.model

object ml extends Serializable {

  case class ProbabilitiesByState(state: String, probabilities: Seq[StateProbability])

  case class StateProbability(state: String, probability: Double) {
    override def toString: String = state + ": " + probability
  }

}
