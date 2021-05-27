from dbclient import *

# python 3.6

reset_schedules = False


def is_excluded_cluster(cinfo):
    if cinfo is None:
        return False
    keep_alive_tags = ["keepalive", "keep_alive"]
    if cinfo.get('custom_tags', None):
        tag_keys = [tag.lower() for tag in cinfo['custom_tags'].keys()]
        if ("keepalive" in tag_keys) or ("keep_alive" in tag_keys):
            return True
        else:
            return False
    else:
        return False


def cleanup_jobs(url, token, env_name):
    c_types = ['env_name', 'excluded', 'scheduled', 'long_running', 'empty_job', 'multitask_job']
    report = dict([(key, []) for key in c_types])
    report['env_name'] = (env_name, url)
    # Simple class to list versions and get active cluster list

    print("Cleaning up jobs. Getting the jobs client ....")
    jclient = JobsClient(token, url)

    # get the jobs that have run more than 4 hours
    print("Cleaning up jobs. Get jobs duration ....")
    long_jobs_list = jclient.get_jobs_duration(4)

    # get scheduled jobs
    print("Get scheduled jobs ....")
    sjobs = jclient.get_scheduled_jobs()

    # get multitask jobs
    print("Get multi-task jobs ....")
    mt_jobs = jclient.get_multitask_jobs()
    for mt_job in mt_jobs:
        report['multitask_job'].append(mt_job)

    # get duplicate jobs by name
    djobs = jclient.get_duplicate_jobs()

    # get empty / untitled jobs
    print("Get empty jobs ....")
    empty_jobs = jclient.find_empty_jobs()
    print("Finished empty jobs!")
    hour_min = datetime.datetime.now().strftime("%H_%M")

    print("# Delete duplicate job names")
    deleted_job_ids = set()
    for job_name, job_ids in djobs.items():
        # delete older job ids, keep the first job id
        for v in job_ids[1:]:
            if v not in deleted_job_ids:
                deleted_job_ids.add(v)
                resp = jclient.delete_job(v)

    print("# Long running jobs\n")
    for job in long_jobs_list:
        print("Long running job: {0}\t User: {1}".format(job['job_id'], job['creator_user_name']))
        if is_excluded_cluster(job.get('cluster', None)):
            report['excluded'].append(job)
        else:
            report['long_running'].append(job)

    print("# Repeated scheduled jobs\n")
    for job in sjobs:
        # if job is created by HLS team, exclude from periodic cleanup
        print("Scheduled job: {0}\t User: {1}\t Schedule: {2}".format(job['job_id'], job['creator_user_name'],
                                                                      job['schedule']))
        if is_excluded_cluster(job.get('cluster', None)):
            report['excluded'].append(job)
        else:
            report['scheduled'].append(job)

    print("# Empty and Untitled jobs\n")
    for job in empty_jobs:
        print("Empty job: {0}".format(job['job_id']))
        report['empty_job'].append(job)

    if reset_schedules:
        for k, v in report.items():
            if k in ('excluded', 'env_name', 'empty_job', 'multitask_job'):
                continue
            elif k == 'scheduled':
                for job in v:
                    jclient.reset_job_schedule(job['job_id'])
            elif k == 'long_running':
                for job in v:
                    jclient.kill_run(job['run_id'])

        print("# Cleanup Empty Jobs\n")
        for job in report['empty_job']:
            if job['job_id'] not in deleted_job_ids:
                deleted_job_ids.add(job['job_id'])
                jclient.delete_job(job['job_id'])

    return report


def lambda_handler(event, context):
    # get a list of configurations in json format
    configs = get_job_configs()
    envs = {}
    log_bucket = []
    for env in configs:
        envs[env['desc']] = (env['url'], env['token'])
        log_bucket.append(env['s3_bucket'])

    # logging configuration
    bucket_name = log_bucket[0]
    table_name = "job_usage_logs"

    full_report = ""
    html_report = ""
    for x, y in envs.items():
        print("Running jobs cleanup for {0}\n".format(y[0]))
        jobs_report = cleanup_jobs(y[0], y[1], x)
        log_to_s3(bucket_name, table_name, jobs_report)
        full_report += pprint_j(jobs_report)
        full_report += "\n######################################################\n"
        html_report += get_html(jobs_report)

    print(full_report)
    email_list = ["mwc@databricks.com"]
    send_email("Databricks Automated Jobs Usage Report", email_list, full_report, html_report)
    # Print Spark Versions
    message = "Completed running cleanup across all field environments!"
    return {
        'message': message
    }
