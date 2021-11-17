#!/usr/bin/env bash

function log() {
  echo -e "$(date +"%Y-%m-%d %H:%M:%S")" "$1" "$2"
}

function debug() {
  log "DEBUG" "$1"
}

function info() {
  log "INFO" "$1"
}

function warning() {
  log "WARNING" "$1"
}

function error() {
  log "ERROR" "$1"
}

export -f debug
export -f info
export -f warning
export -f error