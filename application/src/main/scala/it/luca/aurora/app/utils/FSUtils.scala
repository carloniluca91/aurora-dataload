package it.luca.aurora.app.utils

import it.luca.aurora.core.logging.Logging
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.fs.{FileStatus, FileSystem, FileUtil, Path}
import org.apache.spark.sql.SparkSession

import java.io.IOException

object FSUtils
  extends Logging {

  def getFileSystem(sparkSession: SparkSession): FileSystem = FileSystem.get(sparkSession.sparkContext.hadoopConfiguration)

  def getValidFilesWithin(fs: FileSystem, directoryPath: Path, fileNameRegex: String): Seq[FileStatus] = {

    val fileStatuses: Seq[FileStatus] = fs.listStatus(directoryPath)
    val isValidInputFile: FileStatus => Boolean = f => f.isFile && f.getPath.getName.matches(fileNameRegex)
    val invalidInputPaths: Seq[FileStatus] = fileStatuses.filterNot { isValidInputFile }
    if (invalidInputPaths.nonEmpty) {

      val fileOrDirectory: FileStatus => String = x => if (x.isDirectory) "directory" else "file"
      val invalidInputPathsStr = s"${invalidInputPaths.map { x => s"  Name: ${x.getPath.getName} (${fileOrDirectory(x)}})" }.mkString("\n")}"
      log.warn(s"Found ${invalidInputPaths.size} invalid file(s) (or directories) at path $directoryPath.\n$invalidInputPathsStr")
    }

    fileStatuses.filter { isValidInputFile }
  }

  @throws[IOException]
  def moveFileToDirectory(fs: FileSystem, filePath: Path, directoryPath: Path): Unit = {

    log.info(s"Moving input file $filePath to $directoryPath")
    if (!fs.exists(directoryPath)) {
      log.warn(s"Target directory $directoryPath does not exist. Creating it now")
      fs.mkdirs(directoryPath, new FsPermission(644.toShort))
      log.info(s"Successfully created target directory $directoryPath")
    }

    FileUtil.copy(fs, filePath, fs, directoryPath, true, fs.getConf)
    log.info(s"Successfully moved input file $filePath to target directory $directoryPath")
  }
}
