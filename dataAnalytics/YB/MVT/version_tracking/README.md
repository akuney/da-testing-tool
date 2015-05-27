# MVT Version Tracking with R

### <span style="color:red">Download the Package</span>
  * copy the compressed ```version_tracking.zip``` file from NAS1 (Intent Media Shared/Data/) 
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
  * in version_tracking folder, there is a file called ```version_tracking```
  * double-click the file will start the process
  * (for the first-time only) it will ask you the database username and password, which you use to connect HP Vertica and MySQL databases
  * at the end of the process, it will output a csv file to your local desktop folder
---

### <span style="color:red">Use the excel sheet</span>
  * There is a excel file in version_tracking folder, namely ```mvt_version_tracking_template.xlsx```
  * In the excel file, there are two sheets, data and pivot
    * ```data``` sheet is where you will paste the csv output, which will be in your desktop
    * ```pivot``` sheet is where the analysis result is. All you need is refresh all pivot tables by:
      * click anywhere in the pivot table
      * click the pivot tab (from the top)
      * on the menu bar, find ```refresh``` option and do ```refresh all```
      
    * It is going to be most useful if you perform filters to each site and multivariate_test_attribute for certain month and year

---

<span style="color:blue">if you have any question, please contact tableau@intentmedia.com or analytics@intentmedia.com</span>