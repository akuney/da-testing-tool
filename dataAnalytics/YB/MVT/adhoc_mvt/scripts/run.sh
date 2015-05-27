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
adUnitType=$(get_input "ad unit type in uppercase letters (i.e. CT, SSR, or META)")
productCategoryType=$(get_input "product category type in uppercase letters (i.e. ALL if it is not product category specific")
attribute=$(get_input "MVT attribute name in uppercase letters with underscores (i.e. NULL if it is not IM MVT test")
startPointer=$(get_input "starting MVT version id or starting date (i.e. 2000 or 2015-01-01")
endPointer=$(get_input "ending MVT version id or ending date (if it is ongoing test, enter NULL")
pubSettingsActive=$(get_input "enter TRUE if we measure based on pub parameter, otherwise FALSE")
placeholderAttribute=$(get_input "if we use pub parameter, then enter the parameter (i.e. site_reporting_value_01 or NULL)")
echo $site
echo $adUnitType
echo $productCategoryType
echo $attribute
echo $startPointer
echo $endPointer
echo $pubSettingsActive
echo $placeholderAttribute
./adhoc_mvt_vBeta.R $site $adUnitType $productCategoryType $attribute $startPointer $endPointer $pubSettingsActive $placeholderAttribute
