from dbclient import *
from cron_descriptor import get_description, FormatException
import json, datetime


class JobsClient(dbclient):

    def get_jobs_list_details(self):
        """
        new function to get all job details include multi-task jobs
        job details must be fetched manually
        """
        j_resp = self.get('/jobs/list', version='2.1')
        offset = 0
        jobs_list = j_resp.get('jobs', [])
        while j_resp.get('has_more'):
            offset = len(jobs_list)
            j_resp = self.get(f'/jobs/list?offset={offset}', version='2.1')
            jobs_list = jobs_list + j_resp.get('jobs', [])
        # with the latest rollout, we can no longer have empty tasks / jobs so we don't need to check for this
        # all jobs have been migrated to multi-task jobs
        job_ids_list = list(map(lambda x: x.get('job_id'), jobs_list))
        all_job_details = []
        for job_id in job_ids_list:
            job_details = self.get(f'/jobs/get?job_id={job_id}', version='2.1')
            del job_details['http_status_code'] # del the http response code since it's not necessary
            all_job_details.append(job_details)
        return all_job_details

    def get_jobs_list(self, print_json=False):
        """ Returns an array of json objects for jobs """
        jobs = self.get("/jobs/list", print_json).get('jobs', [])
        return jobs

    def delete_job(self, job_id=None):
        resp = self.post('/jobs/delete', {"job_id": job_id})
        return resp

    def get_job_id(self, name):
        jobs = self.get_jobs_list_details()
        for i in jobs:
            if i['settings']['name'] == name:
                return i['job_id']
        return None

    def get_multitask_jobs(self):
        """
        get a list of multi-task jobs and log them for reporting
        """
        job_list = self.get_jobs_list_details()

        mt_jobs = []
        for job in job_list:
            if self.is_job_multitask_job(job):
                mt_jobs.append(job)
        return mt_jobs

    def is_excluded_job_by_tag(self, job_id):
        job_details = self.get(f'/jobs/get?job_id={job_id}')

        job_tags = job_details.get('settings').get('tags', {})
        if job_tags:
            lower_keys = list(map(lambda y: y.lower(), job_tags.keys()))
            if 'keepalive' in lower_keys:
                print("Skipping killing job due to KeepAlive tag on job.")
                return True
        return False

    def get_jobs_duration(self, run_time=0):
        """ get running jobs list for jobs running over N hours """
        # get current time
        now = datetime.datetime.utcnow()
        running_jobs_list = self.get('/jobs/runs/list?active_only=true').get('runs', [])
        job_list = []
        for x in running_jobs_list:
            run_obj = dict()
            run_obj['run_id'] = x['run_id']
            # store datetime in str format to serialize into json for logging purposes.
            run_obj['start_time'] = str(datetime.datetime.utcfromtimestamp(x['start_time'] // 1000))
            run_obj['creator_user_name'] = x.get('creator_user_name', 'unknown')

            # If its a spark-submit job, it doesn't contain a job_id parameter. continue with other jobs.
            jid = x.get('job_id', None)
            if jid == None:
                continue
            else:
                run_obj['job_id'] = jid
            # get the run time for the job
            start_dt_obj = datetime.datetime.strptime(run_obj['start_time'], '%Y-%m-%d %H:%M:%S')
            # get the time delta in seconds
            rt = now - start_dt_obj
            hours_run = rt.total_seconds() / 3600
            run_obj['hours_run'] = hours_run
            if hours_run > run_time:
                # return a list of job runs that we need to stop using the `run_id`
                job_list.append(run_obj)
        return job_list

    def kill_run(self, run_id=None):
        """ stop the job run given the run id of the job """
        if run_id is None:
            raise Exception("Invalid run_id")
        else:
            resp = self.post('/jobs/runs/cancel', {"run_id": run_id})
            # Grab the run_id from the result
            pprint_j(resp)

    @staticmethod
    def is_all_empty(job_details):
        # get task attributes: https://docs.databricks.com/api/latest/jobs.html#request-structure
        is_spark_jar_task = job_details['settings'].get('spark_jar_task', None)
        is_notebook_task = job_details['settings'].get('notebook_task', None)
        is_python_task = job_details['settings'].get('spark_python_task', None)
        is_spark_submit_task = job_details['settings'].get('spark_submit_task', None)
        is_multitask_task = job_details['settings'].get('tasks', None)
        is_pipeline_task = job_details['settings'].get('pipeline_task', None)
        # OR all operations to find whether we have a single defined tasks
        all_tasks = [is_spark_jar_task,
                     is_notebook_task,
                     is_python_task,
                     is_spark_submit_task,
                     is_multitask_task,
                     is_pipeline_task]
        is_job_all_empty = all(v is None for v in all_tasks)
        return is_job_all_empty

    def find_empty_jobs(self):
        jobs = self.get_jobs_list_details()
        # look for jobs without titles
        untitled_jobs = list(filter(lambda x: x['settings']['name'] == "Untitled", jobs))
        # look for jobs without any tasks
        empty_jobs = list(filter(lambda x: self.is_all_empty(x), jobs))
        # find the creators of this job to see how often these users create empty jobs
        creators_untitled = list(map(lambda x: x.get('creator_user_name', 'unknown'), untitled_jobs))
        creators_empty = list(map(lambda x: x.get('creator_user_name', 'unknown'), empty_jobs))
        # convert into a set to remove duplicates from the list
        empty_job_ids = list(map(lambda x: {'job_id': x['job_id'], 'creator': x.get('creator_user_name', 'unknown')},
                            empty_jobs + untitled_jobs))
        unique_empty_jobs = [dict(t) for t in set([tuple(d.items()) for d in empty_job_ids])]
        return unique_empty_jobs

    @staticmethod
    def is_job_dlt(job_details):
        """
        check if this is a delta live tables job by looking at the defined spark configs
        """
        job_settings = job_details.get('settings')
        cluster = job_settings.get('new_cluster', '')
        # check that the job runs on a new cluster
        if cluster:
            # check if spark config exists
            spark_conf = cluster.get('spark_conf', '')
            if spark_conf:
                for conf, value in spark_conf.items():
                    if 'pipelines.id' == conf:
                        return True
        return False

    @staticmethod
    def is_job_multitask_job(job_details):
        """
        check if there's multi-task job definitions
        the settings would have a list of `tasks` to define each step
        """
        if 'tasks' in job_details.get('settings'):
            return True
        return False

    def get_scheduled_jobs(self):
        # Grab job templates
        run_list = self.get_jobs_list_details()

        jobs_list = []
        if run_list:
            # Filter all the jobs that have a schedule defined
            scheduled_jobs = list(filter(lambda job: 'schedule' in job['settings'], run_list))
            for x in scheduled_jobs:
                y = dict()
                y['creator_user_name'] = x.get('creator_user_name', 'unknown')
                y['job_id'] = x['job_id']
                y['job_name'] = x['settings']['name']
                y['created_time'] = datetime.datetime.fromtimestamp(x['created_time'] / 1000.0).strftime(
                    '%Y-%m-%d %H:%M:%S.%f')
                job_settings = x.get('settings', None)
                job_schedule = job_settings.get('schedule', None)
                try:
                    readable_schedule = get_description(job_schedule.get('quartz_cron_expression'))
                    y['schedule'] = readable_schedule
                except FormatException:
                    y['schedule'] = job_schedule.get('quartz_cron_expression')
                new_cluster_conf = x.get('settings', None).get('new_cluster', None)
                if new_cluster_conf is not None:
                    # job is configured to run on a new cluster, we can check keep alive tags
                    new_cluster_conf['creator_user_name'] = x.get('creator_user_name', 'unknown')
                    y['cluster'] = new_cluster_conf
                jobs_list.append(y)
        return jobs_list

    def reset_job_schedule(self, job_id=None):
        if job_id is not None:
            resp = self.get('/jobs/get?job_id={0}'.format(job_id))
            print("Job template: ")
            if resp.get('http_status_code') != 200:
                print("ERROR:")
                pprint_j(resp)
                return
            pprint_j(resp)
            job_tags = resp.get('settings').get('tags', {})
            if job_tags:
                lower_keys = list(map(lambda y: y.lower(), job_tags.keys()))
                if 'keepalive' in lower_keys:
                    print("Skipping resetting schedule due to KeepAlive tag on job.")
                    return
            if resp.get("error_code", None) == "INVALID_PARAMETER_VALUE":
                print("Job id was removed: {0}".format(job_id))
                return
            # Remove the created_time field
            resp.pop('created_time', None)
            # Pop off the job settings from the results structure
            settings = resp.pop('settings', None)
            # Grab the current schedule
            schedule = settings.pop('schedule', None)
            print("Defined schedule: ")
            print(schedule)
            # set the schedule to paused
            schedule['pause_status'] = 'PAUSED'
            settings['schedule'] = schedule
            # Define the new config with the created_time removed
            new_config = resp
            new_config['new_settings'] = settings
            print("Applying new config without schedule: ")
            pprint_j(new_config)

            resp = self.post('/jobs/reset', new_config)
            # Grab the run_id from the result
            pprint_j(resp)
        else:
            print("Invalid job id")

    def get_duplicate_jobs(self):
        job_dups = {}
        jl = self.get_jobs_list_details()
        for job in jl:
            jname = job['settings']['name']
            jid = job['job_id']
            if job_dups.get(jname, None) is None:
                job_dups[jname] = [jid]
            else:
                job_dups[jname] = sorted(job_dups[jname] + [jid])

        duplicate_jobs = {k: v for k, v in job_dups.items() if len(v) > 1}
        return duplicate_jobs

