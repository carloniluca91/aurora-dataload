package it.luca.aurora.configuration.metadata.transform

import it.luca.aurora.configuration.metadata.DeserializationTest

class PartitioningTest
  extends DeserializationTest {

  s"A ${nameOf[Partitioning]}" should
    s"be deserialized as an instance of ${nameOf[FileNameRegexPartitioning]} when ${Partitioning.Type} = ${Partitioning.FileNameRegex}" in {


    // TODO
  }
}
