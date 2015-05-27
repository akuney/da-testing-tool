# Adhoc MVT Analysis with R

### <span style="color:red">Download the Package</span>
  * copy the compressed ```adhoc_mvt.zip``` file from NAS1 (Intent Media Shared/Data/)
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

  * it will then ask you to enter parameters (below)
    * ```Site``` will be the name of the site in capital letters
    * ```Ad Unit Type``` will the name of the product in capital letters (i.e. CT, SSR, or META)
    * ```Product Category Type``` will be the name of product category type in capital letters (i.e. FLIGHTS or HOTELS)
    * ```MVT Attribute``` is the name of the MVT Attribute in capital letter with underscores
    * ```Start MVT Version``` is the starting multivariate_version_id
    * ```End MVT Version``` is the ending multivariate_version_id (it can be null if it is an ongoing test)
    * ```Pub Settings Active``` is true/false flag indicating whether we are using publisher parameters
    * ```Placeholder Attribute``` is the attribute when we are using publisher parameters (i.e. site_reporting_value_01)
    
      * Example: TRAVELZOO CT FLIGHTS FLIGHTS_ADVERTISEMENT_SELECT_ALL_TEXT 2188 NULL FALSE NULL
    
  * There are two methodologies in the script, one ```with search form event``` and another ```without search form event```

  * The script will choose which methodology to use based on the MVT Attribute you enter
  
    * IF the MVT Attribute contains ```SEARCH_FORM``` or ```SUPERSEARCH_DESIGN``` or ```FLIGHTS_EXIT_OVERLAY_TRIGGER``` or ```FLIGHTS_ADVERTISEMENT_SELECT_ALL_TEXT```
      THEN the script will use the one with search form event. Otherwise, it will use the one without the search form event.

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
      
    * ```MVT_Analysis_with_SF_v2.xlsx``` contains all media funnel metrics starting from search form event to gross media revenue
    * ```MVT_Analysis_without_SF_v3.xlsx``` contains the same metrics except for search form related ones

---

<span style="color:blue">if you have any question, please contact yoojong.bang@intentmedia.com</span>