# Adhoc MVT Analysis with R

### <span style="color:red">Download the Package</span>
  * copy the compressed ```adhoc_mvt.zip``` file from NAS1 
    * (shared drive which can be found at the left bar in finder window)
  * paste the file to your local disk space
  * double-clicking the file will extract the file and create a folder

---

### <span style="color:red">Install R to your local machine</span>
  * in adhoc_mvt folder, there is a file called ```R-3.1.1-mavericks.pkg```
  * double-click the file will start the install
  * follow the install process (usually hitting "continue" multiple times, no special setting required)

---

### <span style="color:red">Use the application</span>
  * in adhoc_mvt folder, there is a file called ```adhoc_mvt```
  * double-click the file will start the process
  * (for the first-time only) it will ask you the database username and password, which you use to connect HP Vertica and MySQL databases

  * it will then ask you ```Site```, ```MVT Attribute```, ```Lookback Period```
    * ```Site``` will be the name of the site in capital letters
    * ```MVT Attribute``` is the name of the MVT Attribute in capital letter with underscores
    * ```Lookback Period``` is the number of days you want to extract the data from
    
      * Example: TRAVELZOO FLIGHTS_SEARCH_FORM_HEADLINE 30
    
    * This example will pull data for 
      ```
      Publisher = Travelzoo
      MVT Attribute = FLIGHTS_SEARCH_FORM_HEADLINE
      Lookback period = 30 days
      ```
  * There are two methodologies in the script, one ```with search form event``` and another ```without search form event```

  * The script will choose which methodology to use based on the MVT Attribute you enter
  
    * IF the MVT Attribute contains ```SEARCH_FORM``` or ```SUPERSEARCH_TOP20_HEADLINE_SIZE``` THEN the script will use the one with search form event. Otherwise, it will use the one without the search form event.

  * After the script finishes, it will automatically create a csv file on your desktop containing the data

---

### <span style="color:red">Use the excel sheet</span>
  * There are two excel files in the adhoc_mvt folder, namely ```MVT_Analysis_with_SF.xlsx``` and ```MVT_Analysis_without_SF.xlsx```
  * In each excel file, there are two sheets, data and pivot
    * ```data``` sheet is where you will paste the csv output, which will be in your desktop
    * ```pivot``` sheet is where the analysis result is. All you need is refresh all pivot tables by:
      * click any one of the pivot table
      * click the pivot tab (from the top)
      * on the menu bar, find ```refresh``` option and do ```refresh all```
      
    * ```MVT_Analysis_with_SF.xlsx``` contains Form IR, IR, IPU, CTR, CPU, RPAC, RPU
    * ```MVT_Analysis_without_SF.xlsx``` contains the same metrics except for Form IR

---

<span style="color:blue">if you have any question, please contact tableau@intentmedia.com</span>