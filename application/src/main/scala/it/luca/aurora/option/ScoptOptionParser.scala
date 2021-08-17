package it.luca.aurora.option

import scopt.OptionParser

object ScoptOptionParser extends OptionParser[CliArguments]("scopt 4.0"){

  opt[String](ScoptOption.YamlFile.shortOption, ScoptOption.YamlFile.longOption)
    .text(ScoptOption.YamlFile.description)
    .required()
    .validate(s => if (s.endsWith(".yaml")) success else failure(s"A .yaml file is expected. Found $s"))
    .action((s, c) => c.copy(yamlFileName = s))

  opt[String](ScoptOption.DataSource.shortOption, ScoptOption.DataSource.longOption)
    .text(ScoptOption.DataSource.description)
    .required()
    .action((s, c) => c.copy(dataSource = s))
}
