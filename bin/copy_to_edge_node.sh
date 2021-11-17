#!/usr/bin/env bash

. bin/setup.sh

function usage() {

  script_name=$(basename "$0")
  echo "Usage: $script_name [-u user] [-p password] [-l localfile] [-r remotepath]"
  echo "  -u, --user        User name for remote connection"
  echo "  -p, --password    Password for remote connection"
  echo "  -l, --local       Path to local file to be copied on remote server"
  echo "  -r, --remote      Remote path within which local file will be copied"

}

# If no argument has been provided, print usage and return
if [ -z "$1" ]
then
  usage;
else
  args="$*"
  while [[ $# -gt 0 ]]
  do
    key="$1"
    case $key in

      # options
      "-h"|"--help") usage; break ;;
      "-u"|"--user") user="$2"; shift; shift ;;
      "-p"|"--password") password="$2"; shift; shift ;;
      "-l"|"--localfile") localfile="$2"; shift; shift ;;
      "-r"|"--remotepath") remotepath="$2"; shift; shift ;;
      *) warning "Unrecognized option ($key)"; shift ;;
    esac
  done

  if [ -n "$user" ] && [ -n "$password" ] && [ -n "$localfile" ] && [ -n "$remotepath" ];
  then
    info "Moving local file $localfile to remotepath $remotepath as user $user"
    pscp -l "$user" -pw "$password" "$localfile" quickstart-bigdata:"$remotepath"
    return_value=$?
    if [[ $return_value == 0 ]];
    then info "Moved local file $localfile to remotepath $remotepath as user $user";
    else error "Could not move local file $localfile to remotepath $remotepath as user $user";
    fi
  else
    error "Partial arguments provided [$args]. Could not fulfill request"
    usage
  fi
fi