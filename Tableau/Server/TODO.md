TODO
====

A list of identified issues in this codebase. These are archived here as notes for future developers and to allow us to clean up Pivotal Tracker issues.

## Adding a new recipient to a report?

If you add a new recipient to a report, make sure to add the appropriate entry in `entities_to_recipients.ps1`.  As part of [84333854: Stop generating weekly reports for paused advertisers](https://www.pivotaltracker.com/n/projects/1184010/stories/84333854), we removed an advertiser (Fly.com UK) who no longer receives a report, but *has data* as part of another report (the SCA Publisher Performance Report).  If Fly.com UK wanted a report delivered directly to them again, we'd need to add them back to the `entities_to_recipients.ps1` list.

## [80731438: Investigate race conditions in logging](https://www.pivotaltracker.com/story/show/80731438)

> Multiple scripts are failing with `Failed with: The process cannot access the file 'C:\data\Tableau\server\Logs\Reports\External_CrossProductReport.log' because it is being used by another process..`
>
> We should investigate individual log files for each process.

## [80383918: Fix issue where multiple PowerShell processes run for same job](https://www.pivotaltracker.com/story/show/80383918)

## [81958562: Address the fact that we sometimes get duplicate internal dashboard reports in the morning](https://www.pivotaltracker.com/story/show/81958562)

<small>Sanitized email addresses out of this email thread</small>

> _Here's an email thread - not sure of priority yet...._
>
>That's an excellent point.  I'll make a note about it.
>
>Cheers
>Dan
>
>On Thu, Oct 30, 2014 at 1:40 PM, Angus Wilson wrote:
>But - I wonder if worth an email to those on the distribution lists for these reports, to explain this is a known issue so people don’t keep reporting it to you?
>
>Angus 
>
>On Oct 30, 2014, at 1:38 PM, Daniel Wellman wrote:
>
>HI Angus,
>
>Yes, those internal reports do sometimes get sent out twice.  Since this only affects reports we send to people at Intent Media, we haven't prioritized fixing it.  If this ends up causing a serious impact, let us know so we can take this in to our story >prioritization process.
>
>Cheers!
>Dan
>
>On Thu, Oct 30, 2014 at 9:19 AM, Angus Wilson wrote:
>
>Hi,
>
>Lately I’ve been receiving 2 of these each day?
>
>Angus 

## [81866316: Migrate Report Titles and Workbooks to use new Product Names](https://www.pivotaltracker.com/story/show/81866316)

>Yoojong said that he's been asked to change the workbook and report titles of the reports we send to our clients.
>
>- This will require changing workbook names, which will require changing the Powershell.  
>- It will also mean changing the titles of the e-mails we send.  
>- Additionally, we'll need to change the Google Doc spreadsheet expected report titles to help us keep our safety scripts working.
>
>This will be a fair amount of effort to do; Yoojong offered to come sit with us when we make this change so we can do the work in synch.
