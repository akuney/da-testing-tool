#!/usr/bin/env bash

function update_credentials ()
{
  credential_source=$(find $HOME/ -maxdepth 1 -type f | grep ".adhoc_mvt.properties")
  if [ ${#credential_source} == 0 ]; then
    username=$(get_input "enter your database username")
    echo "username = ${username}" >> $HOME/.adhoc_mvt.properties
    password=$(get_input "enter your database password")
    echo "password = ${password}" >> $HOME/.adhoc_mvt.properties
  fi
}

update_credentials
./version_tracking.R
