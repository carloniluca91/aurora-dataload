package it.luca.aurora.core.implicits

import it.luca.aurora.core.Logging
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.fs.{FileStatus, FileSystem, FileUtil, Path}

import scala.io.Source
import scala.util.matching.Regex

class FileSystemWrapper(protected val fs: FileSystem)
  extends Logging {

  /**
   * Return list of [[FileStatus]] related to files within given directory path whose name matches given regex
   * @param dirPath [[Path]] of directory to check
   * @param fileNameRegex [[Regex]] to be matched by a valid file's name
   * @return list of [[FileStatus]]
   */

  def getMatchingFiles(dirPath: Path, fileNameRegex: Regex): Seq[FileStatus] = {

    val entitiesWithinDirPath: Seq[FileStatus] = fs.listStatus(dirPath)
    val isValidInputFile: FileStatus => Boolean = f => f.isFile && fileNameRegex.findFirstMatchIn(f.getPath.getName).isDefined
    val invalidEntities: Seq[FileStatus] = entitiesWithinDirPath.filterNot { isValidInputFile }
    if (invalidEntities.nonEmpty) {

      // Log name of invalid files or directories
      val entityType: FileStatus => String = f => if (f.isDirectory) "directory" else "file"
      val invalidEntitiesStr = s"${invalidEntities.map { e => s"  Name: ${e.getPath.getName} (${entityType(e)}})" }
        .mkString("\n")}".concat("\n")
      log.warn(s"Found ${invalidEntities.size} invalid file(s) (or directories) at path $dirPath.\n$invalidEntitiesStr")
    }

    entitiesWithinDirPath.filter { isValidInputFile }
  }

  /**
   * Modify HDFS permissions for table's location and partitions if they do not match given permissions
   * @param tableLocation HDFS location of a table (obtained using [[it.luca.aurora.core.implicits.SparkSessionWrapper.getTableLocation]]
   * @param permission [[FsPermission]] to match
   * @return true if an action was necessary, false otherwise
   */

  def modifyTablePermissions(tableLocation: String, owner: String, permission: FsPermission): Boolean = {

    val tableLocationPath = new Path(tableLocation)
    val tableLocationStatus: FileStatus = fs.getFileStatus(tableLocationPath)
    val belongsToOwner: FileStatus => Boolean = f => f.getOwner.equalsIgnoreCase(owner)
    val isDirBelongingToOwnerWithDifferentPermissions: FileStatus => Boolean = f => f.isDirectory && belongsToOwner(f) && !f.getPermission.equals(permission)
    val (tableName, givenPermissions): (String, String) = (tableLocationPath.getName, s"given permissions ($permission)")
    val anyActionOnTableLocation: Boolean = if (isDirBelongingToOwnerWithDifferentPermissions(tableLocationStatus)) {

      log.info(s"Location of table $tableName ($tableLocation) does not match $givenPermissions")
      fs.setPermission(tableLocationPath, permission)
      log.info(s"Successfully set permissions to $permission on location $tableLocationPath")
      true
    } else {
      val rationale: String = if (!belongsToOwner(tableLocationStatus))
        s"as it belongs to a different owner (${tableLocationStatus.getOwner})"
      else s"as it belongs to given owner ($owner) but match $givenPermissions"
      log.info(s"No action to apply on root location of table $tableName ($tableLocation) $rationale")
      false
    }

    // Table partitions
    log.info(s"Checking status of partitions of table $tableName")
    val matchingPartitions: Seq[FileStatus] = fs.listStatus(tableLocationPath).filter { isDirBelongingToOwnerWithDifferentPermissions }
    val anyActionOnTablePartitions: Boolean = if (matchingPartitions.nonEmpty) {

      val tablePartitionsString: String = matchingPartitions.map { p => s"  ${p.getPath.getName}" }.mkString("\n").concat("\n")
      log.info(s"Found ${matchingPartitions.size} partition(s) for table $tableName with owner $owner that do not match " +
        s"$givenPermissions.\n\n$tablePartitionsString")
      matchingPartitions.foreach { p =>
        fs.setPermission(p.getPath, permission)
        log.info(s"Successfully set permissions to $permission for partition ${p.getPath.getName}")
      }
      true
    } else {
      log.info(s"No action will be applied on table partitions of $tableName as they do not belong to owner $owner or already match $givenPermissions")
      false
    }

    anyActionOnTableLocation || anyActionOnTablePartitions
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
      log.warn(s"Target directory $targetDir does not exist. Creating it now with permissions $fsPermission")
      fs.mkdirs(targetDir, fsPermission)
      log.info(s"Successfully created target directory $targetDir with permissions $fsPermission")
    }

    FileUtil.copy(fs, file, fs, targetDir, true, fs.getConf)
    log.info(s"Successfully moved input file $file to target directory $targetDir")
  }

  /**
   * Read content of file at given path as a single string
   * @param path [[Path]] to be read
   * @return string representing content of given file
   */

  def readHDFSFileAsString(path: String): String = {

    Source.fromInputStream(fs.open(new Path(path)))
      .getLines()
      .mkString(" ")
  }
}
