import json, requests, datetime

global pprint_j


# Helper to pretty print json
def pprint_j(i):
    print(json.dumps(i, indent=4, sort_keys=True))


def get_job_configs(config_path='config/job.conf'):
    config_list = []
    with open(config_path, 'r') as fp:
        for env in fp:
            env_conf = json.loads(env)
            config_list.append(env_conf)
    return config_list


class dbclient:
    """A class to define wrappers for the REST API"""

    def __init__(self, token="ABCDEFG1234", url="https://myenv.cloud.databricks.com"):
        self._token = {'Authorization': 'Bearer {0}'.format(token)}
        self._url = url

    def test_connection(self):
        results = requests.get(self._url + 'api/2.0/clusters/spark-versions', headers=self._token)
        http_status_code = results.status_code
        if http_status_code != 200:
            print("Error. Either the credentails have expired or the credentials don't have proper permissions.")
            print("If you have a ~/.netrc file, check those credentials. Those take precedence over passed input.")
            print(results.text)
            return -1
        return 0

    def get(self, endpoint, json_params={}, printJson=False, version='2.0'):
        if version:
            ver = version
        if json_params:
            raw_results = requests.get(self._url + '/api/{0}'.format(ver) + endpoint, headers=self._token,
                                       params=json_params)
            results = raw_results.json()
        else:
            raw_results = requests.get(self._url + '/api/{0}'.format(ver) + endpoint, headers=self._token)
            results = raw_results.json()
        if printJson:
            print(json.dumps(results, indent=4, sort_keys=True))
        results['http_status_code'] = raw_results.status_code
        return results

    def post(self, endpoint, json_params={}, printJson=True, version='2.0'):
        if version:
            ver = version
        if json_params:
            raw_results = requests.post(self._url + '/api/{0}'.format(ver) + endpoint, headers=self._token,
                                        json=json_params)
            results = raw_results.json()
        else:
            print("Must have a payload in json_args param.")
            return {}
        if printJson:
            print(json.dumps(results, indent=4, sort_keys=True))
        # if results are empty, let's return the return status
        if results:
            results['http_status_code'] = raw_results.status_code
            return results
        else:
            return {'http_status_code': raw_results.status_code}
