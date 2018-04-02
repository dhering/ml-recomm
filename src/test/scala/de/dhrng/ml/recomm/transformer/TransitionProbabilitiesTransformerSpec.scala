package de.dhrng.ml.recomm.transformer

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.scalatest._

class TransitionProbabilitiesTransformerSpec extends FeatureSpec with GivenWhenThen with Matchers with
  OptionValues with Inside with Inspectors with BeforeAndAfter {

  def fixture =
    new {
      val session =  SparkSession
        .builder()
        .appName("test")
        .master("local[1]")
        .getOrCreate()
    }

  feature("TransitionProbabilitiesTransformer") {

    scenario("transform to probabilities") {

      Given("a list of 5 product transitions")
      val session = fixture.session
      import session.implicits._

      val itemFrequencies: DataFrame = Seq(
        ("A", "B", 50L),
        ("A", "C", 35L),
        ("A", "D", 15L),
        ("B", "D", 9999L),
        ("B", "E", 1L)
      ).toDF("antecedent", "consequent", "frequency")

      val transitionProbabilitiesTransformer = new TransitionProbabilitiesTransformer(session, 0.0001)

      When("the transitions are transformed to probabilities")
      val dataFrame = transitionProbabilitiesTransformer.transform(itemFrequencies)

      Then("5 transition probabilities are in the result")
      val rows = dataFrame.collect()
      assert(rows.length == 5)
      assert(getRowFor("A", "B", rows).getDouble(2) == 0.5)
      assert(getRowFor("A", "C", rows).getDouble(2) == 0.35)
      assert(getRowFor("A", "D", rows).getDouble(2) == 0.15)
      assert(getRowFor("B", "D", rows).getDouble(2) == 0.9999)
      assert(getRowFor("B", "E", rows).getDouble(2) == 0.0001)

    }
  }

  private def getRowFor(antecedent: String, consequent: String, rows: Array[Row]) = rows.collectFirst { case row
    if (row.getString(0) == antecedent && row.getString(1) == consequent) => row
  }.orNull
}
