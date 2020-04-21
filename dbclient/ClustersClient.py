import time
from dbclient import *


class ClustersClient(dbclient):

    def get_spark_versions(self):
        return self.get("/clusters/spark-versions", printJson=True)

    def get_cluster_list(self, alive=True):
        """ Returns an array of json objects for the running clusters. Grab the cluster_name or cluster_id """
        cl = self.get("/clusters/list", printJson=False)
        if alive:
            running = filter(lambda x: x['state'] == "RUNNING", cl['clusters'])
            for x in running:
                print(x['cluster_name'] + ' : ' + x['cluster_id'])
            return running
        else:
            return cl['clusters']

    def get_long_clusters(self, run_time_hours=0):
        # get current time
        now = datetime.datetime.utcnow()
        cluster_list = self.get('/clusters/list')['clusters']

        # grab the running clusters into a list
        running_cl_list = filter(lambda x: x['state'] == 'RUNNING', cluster_list)
        # kill list array
        long_cluster_list = []
        for x in running_cl_list:
            co = dict()
            co['start_time'] = str(datetime.datetime.utcfromtimestamp(x['start_time'] / 1000))
            co['cluster_name'] = x['cluster_name']
            co['creator_user_name'] = x['creator_user_name']
            co['cluster_id'] = x['cluster_id']
            co['autotermination_minutes'] = x['autotermination_minutes']
            if x.get('custom_tags', None):
                if x.get('custom_tags').get('ResourceClass', None) == "Serverless":
                    co['is_serverless'] = True
            # get the current time of cluster run times
            rt = now - datetime.datetime.utcfromtimestamp(x['start_time'] / 1000)
            hours_run = rt.total_seconds() / 3600
            co['hours_run'] = hours_run
            if (hours_run > 4.0):
                long_cluster_list.append(co)
        return long_cluster_list

    def kill_cluster(self, cid=None):
        """ Kill the cluster id of the given cluster """
        resp = self.post('/clusters/delete', {"cluster_id": cid})
        pprint_j(resp)

    def get_global_init_scripts(self):
        """ return a list of global init scripts """
        ls = self.get('/dbfs/list', {'path': '/databricks/init/'}).get('files', None)
        if ls is None:
            return []
        else:
            global_scripts = [{'path': x['path']} for x in ls if x['is_dir'] == False]
            return global_scripts

    def delete_init_script(self, path=None):
        """ delete the path passed into the api """
        resp = self.post('/dbfs/delete', {'path': path, 'recursive': 'false'})
        pprint_j(resp)

    def is_stream_running(self, cid=None):
        """ check if a streaming job is running on the cluster """
        # Get an execution context id. You only need 1 to run multiple commands. This is a remote shell into the environment
        ec = self.post('/contexts/create', {"language": "scala", "clusterId": cid}, version="1.2")
        # Grab the execution context ID
        ec_id = ec['id']
        # This launches spark commands and print the results. We can pull out the text results from the API
        command_payload = {'language': 'scala',
                           'contextId': ec_id,
                           'clusterId': cid,
                           'command': 'import scala.collection.JavaConverters._ ; Thread.getAllStackTraces().keySet().asScala.filter(_.getName.startsWith("stream execution thread for")).isEmpty'}

        command = self.post('/commands/execute', \
                            command_payload, \
                            version="1.2")
        com_id = command['id']
        result_payload = {'clusterId': cid, 'contextId': ec_id, 'commandId': com_id}
        resp = self.get('/commands/status', result_payload, version="1.2")
        is_running = resp['status']

        # loop through the status api to check for the 'running' state call and sleep 1 second
        while (is_running == "Running"):
            resp = self.get('/commands/status', result_payload, version="1.2")
            is_running = resp['status']
            time.sleep(1)

        # check the final result to see if a streaming job is running
        is_not_streaming = 'false'
        payload = resp['results'].get('data', None)
        if payload is not None:
            is_not_streaming = payload.split('=')[1].strip().lower()
        # this returns false if there is no stream running, otherwise it returns true
        # false: stream is not running
        # true: stream is running
        if is_not_streaming == 'true':
            return False
        else:
            return True
