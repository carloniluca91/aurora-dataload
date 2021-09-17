#!/usr/bin/env bash

function log() {

  echo -e "$(date +"%Y-%m-%d %H:%M:%S")" "[$1]" "$2"

}

log "info" "Starting run_ingestion.sh script"
dataSourceNameOpt="-d"
dataSourceName=""
while [[ $# -gt 0 ]]
do
  key="$1"
  case $key in

      # dataSourceName
      "$dataSourceNameOpt")
        dataSourceName="$2"
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

if [[ -z $dataSourceName ]];
then
  log "error" "Datasource name ($dataSourceNameOpt option) not provided. Spark application will not be submitted"
else

  log "info" "Provided dataSource name: '$dataSourceName'"

  # Spark submit settings
  queue=root.users.osboxes
  applicationName="Aurora Dataload App - $dataSourceName"
  applicationLibDir=hdfs:///user/osboxes/apps/aurora_dataload/lib
  applicationJar="$applicationLibDir/aurora_dataload.jar"
  applicationLog4File=spark_log4j2.xml
  applicationYamlFile=spark_application.yaml

  applicationLog4jXmlPath="$applicationLibDir/$applicationLog4File"
  applicationYamlPath="$applicationLibDir/$applicationYamlFile"
  sparkSubmitFiles=$applicationLog4jXmlPath,$applicationYamlPath

  mainClass=it.luca.aurora.app.Main
  yamlFileOpt="-y"
  mainClassArgs="$yamlFileOpt $applicationYamlFile $dataSourceNameOpt $dataSourceName"

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
    --driver-java-options "-Dlog4j.configurationFile=$applicationLog4File" \
    --driver-class-path /etc/hive/conf \
    --class $mainClass \
    $applicationJar \
    $yamlFileOpt $applicationYamlFile $dataSourceNameOpt "$dataSourceName"

  log "info" "Spark-submit completed"
fi