## Databricks DBU Alerts 

This is a lambda package to alert on DBU usage for long running jobs.   

Automated tool to monitor / cleanup demo environments. Runs hourly during USA working hours to help reduce costs from accidental job configurations during demos.  
Rules:
1. Reset scheduled jobs unless a `keep_alive` tag is specified
2. Delete empty job templates to keep the workspace clean
3. Reset streaming jobs unless tag is defined
4. Delete jobs with duplicate job names

Lambda Requirements:
* Load all dependencies into a zip file
* Load the code base into the same zip file

**Build**:
Run the `rebuild.sh` script to package the zip.   
Use a CloudWatch event trigger in AWS lambda to kickoff the job hourly.   
Ensure you have S3 permissions to write the json logs and analyze later.

