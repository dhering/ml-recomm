package de.dhrng.ml.recomm.writer

import java.io.File

import de.dhrng.ml.recomm.model.ml.{ActionValue, ActionValuesByState}
import org.scalatest.{FeatureSpec, GivenWhenThen, Matchers}

class CsvReaderSpec extends FeatureSpec with GivenWhenThen with Matchers{

  feature("CsvReader") {

    scenario("read CSV file") {

      When("a model is read from a CSV file")
      val model = CsvReader.read("src/test/resources/model-import-test.csv")

      Then("the model has 3 entries")
      assert(model.size == 3)

      And("state A has 5 action values")
      assert(model("A").actionValues.size == 5)

      And("state B has 2 action values")
      assert(model("B").actionValues.size == 2)

      And("state C has 1 action values")
      assert(model("C").actionValues.size == 1)
    }
  }
}
