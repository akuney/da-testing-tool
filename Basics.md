## Basics on Command Line, Git, and more. 

### Tableau

#### Setup the data connection

##### For Vertica
* Enter a server name:
   * production-vertica-cluster-1.internal.intentmedia.net; Port: 5433
* Enter a database on the server:
   * intent_media
* Enter information to log on the database:
   * Username : tableau
   * Password : (tableau server password)

##### For MySQL
* Enter a server name:
   * production-slave-db-server-1.internal.intentmedia.net; Port: 3306
* Enter a database on the server:
   * intent_media_production 
* Enter information to log on the database:
   * Username : tableau
   * Password : (tableau server password)

#### Notes about the dashboards

##### SCA Multiproduct Dashboard
* Addition of New Site and/or Ad Unit
   * Refresh All Extracts FIRST! (important)
   * Go to any worksheet (NOT dashboard)
   * Right Click "Site" (or "Ad Unit") in the parameter list
   * Click "edit"
   * Clicks "clear all" on the bottom right
   * Manually enter "Total" in the first row
   * Click "Add from Field"
   * Click "sca_md__funnel"
   * Click "Site" (or "Ad Unit")
   * Click "OK"
   
   * IF there's a new site being added:
      * Go to any worksheet that is a part of "Performance Benchmarks - Site" dashboard
      * Choose "Site" filter on the right to the newly added site
      * Then, you will see the blue line (instead of black) for the new site in the graph
      * Now, you will click the arrow on the top right of "Site" legend tab
      * Choose "Edit Colors"
      * Choose "Gray" from the dropdown menus in the "Select Color Palette"
      * Click "Assign Palette"
      * Click "Apply" and "OK"
  

### Git

#### Installation & Configuration
* Refer to [Code Wiki - Git Installation] (https://github.com/intentmedia/code/wiki/Git-installation-%26-configuration)

#### Clone to "data" repository
* Refer to [Code Wiki - Dev Env Setup] (https://github.com/intentmedia/code/wiki/Configuring-your-development-environment)
* Specifically, follow "Get the Codebase" section, and replace "code" to "data" because we need to clone the data repo

#### Tutorial
* Refer to [Git Tutorial] (http://gitimmersion.com/)

#### Modify Commit
If we changed the already committed file: 

```terminal
$ git commit --amend
```

```terminal
$ git push origin -f (name of branch to push)
```
* we force the push, and :wq on the log screen

### Command Line

#### find all files that contain a certain string in a folder

```terminal
$ grep -r "string" .
```
* this command will recursively find the file containing the string
