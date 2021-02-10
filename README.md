# ETL Basic Project 
Influenced by:
- https://www.youtube.com/watch?v=i25ttd32-eo&list=PLNkCniHtd0PNM4NZ5etgYMw4ojid0Aa6i
<br>
useful resources:
- http://developers.strava.com/docs/reference/<br>
- https://developers.strava.com/docs/authentication/<br>
- https://medium.com/swlh/using-python-to-connect-to-stravas-api-and-analyse-your-activities-dummies-guide-5f49727aac86<br>
- https://medium.com/swlh/building-an-automated-data-pipeline-using-the-strava-api-30b0ef0fb42f<br>

## ETL process on Strava
Steps:
1) create API application (assuming you have a strava account)<br>
- https://www.strava.com/settings/api
and place the relevant parameters into the settings.py file. ('code' is in step 2)

2) Grab access token by placing the following hyperlink into browser to get code w/ scope (replacing client id and redirect uri)
<br><br>
https://www.strava.com/oauth/authorize?client_id=[CLIENT-ID]&response_type=code&redirect_uri=[REDIRECT_URI]/exchange_token&approval_prompt=force&scope=profile:read_all,activity:read_all
<br><br>
to grab the code in the hyperlink after you press authenicate. Keep that saved until you run the script, which will prompt for the code (only happens for the first time w/o strava_tokens.json file - else you'll have to repeat the process).

3) Run script! if all is well, you have a new/updated file called "my_strava_activities.sqlite"


### Side notes:
Can use [sqlite-viewer](http://inloop.github.io/sqlite-viewer/) to visualise your resulting sqlite file.<br>

Cron is to automate process - weekly -> <br>
    crontab -e  <br>
    0 20 * * 2 env/bin/python strava_etl.py
