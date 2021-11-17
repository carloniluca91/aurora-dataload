#!/usr/bin/env bash

function log() {
  echo -e "$(date +"%Y-%m-%d %H:%M:%S")" "[$1]" "$2"
}

dataSourceIdOpt="-d"
while [[ $# -gt 0 ]]
do
  key="$1"
  case $key in

      # dataSourceId
      "$dataSourceIdOpt") dataSourceId="$2"; shift; shift ;;
      *) log "warning" "Ignoring unrecognized option ($key)"; shift ;;
  esac
done

if [[ -z $dataSourceId ]]
then
  log "error" "Datasource name ($dataSourceIdOpt option) not provided. Spark application will not be submitted"
else

  log "info" "Provided dataSource name: '$dataSourceId'"

  # Spark submit settings
  queue=root.users.osboxes
  applicationName="Aurora Dataload - $dataSourceId"
  applicationLibDir=hdfs:///user/osboxes/apps/aurora_dataload/lib
  applicationJar="$applicationLibDir/aurora_dataload.jar"
  applicationPropertiesFile=spark_application.properties
  dataSourcesFile=aurora_datasources.json
  applicationLog4File=spark_application_log4j.properties

  applicationPropertiesPath="$applicationLibDir/$applicationPropertiesFile"
  dataSourcesFilePath="$applicationLibDir/$dataSourcesFile"
  applicationLog4jPath="$applicationLibDir/$applicationLog4File"
  sparkSubmitFiles=$applicationPropertiesPath,$dataSourcesFilePath,$applicationLog4jPath

  mainClass=it.luca.aurora.app.Main
  propertiesFileOpt="-p"
  dataSourcesFileOpt="-j"
  mainClassArgs="$propertiesFileOpt $applicationPropertiesFile $dataSourcesFileOpt $dataSourcesFile $dataSourceIdOpt $dataSourceId"

  log "info" "Proceeding with spark-submit command. Details:
        applicationName: $applicationName,
        spark submit files: $sparkSubmitFiles,
        main class: $mainClass,
        HDFS jar location: $applicationJar,
        application arguments: $mainClassArgs
      "

  spark-submit --master yarn --deploy-mode cluster \
    --queue $queue \
    --name "$applicationName" \
    --files $sparkSubmitFiles \
    --jars $applicationLibDir/impala-jdbc-driver.jar \
    --driver-java-options "-Dlog4j.configuration=$applicationLog4File" \
    --driver-class-path /etc/hive/conf \
    --class $mainClass \
    $applicationJar \
   $propertiesFileOpt $applicationPropertiesFile $dataSourcesFileOpt $dataSourcesFile $dataSourceIdOpt "$dataSourceId"

  log "info" "Spark-submit completed"
fi