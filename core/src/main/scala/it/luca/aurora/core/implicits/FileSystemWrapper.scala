package it.luca.aurora.core.implicits

import it.luca.aurora.core.Logging
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.fs.{FileStatus, FileSystem, FileUtil, Path}

import scala.io.Source
import scala.util.matching.Regex

class FileSystemWrapper(protected val fs: FileSystem)
  extends Logging {

  /**
   * Return list of [[FileStatus]] related to files within given directory path whose name match given regex
   * @param dirPath [[Path]] of directory to check
   * @param fileNameRegex [[Regex]] to be matched by valid files
   * @return list of [[FileStatus]]
   */

  def getListOfMatchingFiles(dirPath: Path, fileNameRegex: Regex): Seq[FileStatus] = {

    val fileStatuses: Seq[FileStatus] = fs.listStatus(dirPath)
    val isValidInputFile: FileStatus => Boolean = f => f.isFile && fileNameRegex.findFirstMatchIn(f.getPath.getName).isDefined
    val invalidInputPaths: Seq[FileStatus] = fileStatuses.filterNot { isValidInputFile }
    if (invalidInputPaths.nonEmpty) {

      val fileOrDirectory: FileStatus => String = x => if (x.isDirectory) "directory" else "file"
      val invalidInputPathsStr = s"${invalidInputPaths.map { x => s"  Name: ${x.getPath.getName} (${fileOrDirectory(x)}})" }.mkString("\n")}"
      log.warn(s"Found ${invalidInputPaths.size} invalid file(s) (or directories) at path $dirPath.\n$invalidInputPathsStr")
    }

    fileStatuses.filter { isValidInputFile }
  }

  /**
   * Move given file [[Path]] to a target directory [[Path]]
   * @param file [[Path]] of file to move
   * @param targetDir [[Path]] of target directory
   * @param fsPermission [[FsPermission]] to be used for creating target directory (if it does not exist)
   */

  def moveFileToDirectory(file: Path, targetDir: Path, fsPermission: FsPermission): Unit = {

    log.info(s"Moving input file $file to $targetDir")
    if (!fs.exists(targetDir)) {
      log.warn(s"Target directory $targetDir does not exist. Creating it now")
      fs.mkdirs(targetDir, fsPermission)
      log.info(s"Successfully created target directory $targetDir")
    }

    FileUtil.copy(fs, file, fs, targetDir, true, fs.getConf)
    log.info(s"Successfully moved input file $file to target directory $targetDir")
  }

  /**
   * Read content of given [[Path]] as a single string
   * @param path [[Path]] to be read
   * @return string representing content of given file
   */

  def readFileAsString(path: Path): String = {

    Source.fromInputStream(fs.open(path))
      .getLines()
      .mkString(" ")
  }
}
