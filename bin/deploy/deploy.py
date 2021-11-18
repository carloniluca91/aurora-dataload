import logging
import os
import re

from typing import List

from utils import init_logging


def get_build_sbt_version() -> str:

    source_path: str = os.path.dirname(__file__)
    build_sbt_path: str = os.path.join(source_path, "../../build.sbt")
    with open(os.path.abspath(build_sbt_path), "r") as build_sbt:
        version_line: str = [line for line in build_sbt.readlines() if "version" in line][0]
        return re.search("^(ThisBuild / )?version := \"([\\d.]+)\"$", version_line).group(2)


if __name__ == "__main__":

    init_logging(logging.DEBUG)
    log = logging.getLogger(__name__)
    version: str = get_build_sbt_version()
    log.debug(f"Build.sbt version: {version}")
    source_path: str = os.path.dirname(__file__)
    jar_path: str = os.path.join(source_path, f"../../target/scala-2.11/aurora-dataload-{version}.jar")