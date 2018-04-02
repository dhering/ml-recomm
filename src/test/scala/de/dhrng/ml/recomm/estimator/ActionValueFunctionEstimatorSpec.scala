package de.dhrng.ml.recomm.estimator

import de.dhrng.ml.recomm.transformer.FilterTransformer
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest._

class ActionValueFunctionEstimatorSpec extends FeatureSpec with GivenWhenThen with Matchers with
  OptionValues with Inside with Inspectors with BeforeAndAfter {


  def fixture =
    new {
      val session =  SparkSession
        .builder()
        .appName("test")
        .master("local[1]")
        .getOrCreate()
    }

  feature("ActionValueFunctionEstimator") {

    scenario("estimate action value function") {

      Given("a list of 5 user transactions")
      val session = fixture.session
      import session.implicits._

      val transactions: DataFrame = Seq(
        ("A", "B", 0.5D),
        ("A", "C", 0.35D),
        ("A", "D", 0.15D),  // remove this
        ("B", "D", 0.9999D),
        ("B", "E", 0.0001D)      // remove this
      ).toDF("antecedent", "consequent", "confidence")

      val filterTransformer = new ActionValueFunctionEstimator(session )

      When("the transaction list is filtered")
      val model = filterTransformer.fit(transactions).model.collect()

      Then("only 3 transactions should be remain")
      assert(model.length == 5)
    }
  }
}
