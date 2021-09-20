#!/usr/bin/env bash

function log() {

  echo -e "$(date +"%Y-%m-%d %H:%M:%S")" "[$1]" "$2"

}

log "info" "Starting run_ingestion.sh script"
dataSourceIdOpt="-d"
dataSourceId=""
while [[ $# -gt 0 ]]
do
  key="$1"
  case $key in

      # dataSourceId
      "$dataSourceIdOpt")
        dataSourceId="$2"
        shift # past argument
        shift # past value
        ;;

      # unknown option
      *)
        log "warning" "Ignoring unrecognized option ($key) with value $2"
        shift # past argument
        shift # past value
        ;;
  esac
done

if [[ -z $dataSourceId ]];
then
  log "error" "Datasource name ($dataSourceIdOpt option) not provided. Spark application will not be submitted"
else

  log "info" "Provided dataSource name: '$dataSourceId'"

  # Spark submit settings
  queue=root.users.osboxes
  applicationName="Aurora Dataload App - $dataSourceId"
  applicationLibDir=hdfs:///user/osboxes/apps/aurora_dataload/lib
  applicationJar="$applicationLibDir/aurora_dataload.jar"
  applicationPropertiesFile=spark_application.properties
  applicationYamlFile=datasources.yaml
  applicationLog4File=spark_application_log4j.properties

  applicationPropertiesPath="$applicationLibDir/$applicationPropertiesFile"
  applicationYamlPath="$applicationLibDir/$applicationYamlFile"
  applicationLog4Path="$applicationLibDir/$applicationLog4File"
  sparkSubmitFiles=$applicationPropertiesPath,$applicationYamlPath,$applicationLog4Path

  mainClass=it.luca.aurora.app.Main
  propertiesFileOpt="-p"
  yamlFileOpt="-y"
  mainClassArgs="$propertiesFileOpt $applicationPropertiesFile $yamlFileOpt $applicationYamlFile $dataSourceIdOpt $dataSourceId"

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
   $propertiesFileOpt $applicationPropertiesFile $yamlFileOpt $applicationYamlFile $dataSourceIdOpt "$dataSourceId"

  log "info" "Spark-submit completed"
fi