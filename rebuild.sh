#!/bin/bash
yes | rm db_internal_janitor_hourly_jobs.zip 
7z a db_internal_janitor_hourly_jobs.zip *.py ./dep/* dbclient
