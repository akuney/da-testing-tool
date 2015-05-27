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

function get_input ()
{
  return_val="$(osascript -e "Tell application \"System Events\" to display dialog \"Enter the $1:\" default answer \"\"" -e 'text returned of result' 2>/dev/null)"
  if [ $? -ne 0 ]; then
      # The user pressed Cancel
      exit 1 # exit with an error status
  elif [ -z "$return_val" ]; then
      # The user left the project name blank
      osascript -e 'Tell application "System Events" to display alert "You must enter a site name; cancelling..." as warning'
      exit 1 # exit with an error status
  fi
  echo $return_val
}

update_credentials

site=$(get_input "site name in uppercase letters")
adType=$(get_input "ad unit type in uppercase letters (i.e. CT, SSR, or META)")
productCategoryType=$(get_input "product category type in uppercase letters (i.e. FLIGHTS or HOTELS)")
echo $site
echo $adType
echo $productCategoryType
./min_sample_calc.R $site $adType $productCategoryType
