#!/usr/bin/env python
# coding: utf-8

# NPrinting API v2
# Author: Song Yi
import sys
import time
import json
import pytz
import requests
from datetime import datetime
from dateutil import parser
from requests.adapters import HTTPAdapter, Retry
from requests.exceptions import ConnectionError
from requests_ntlm import HttpNtlmAuth

from airflow.models import Variable

est = pytz.timezone('US/Eastern')

class NPrinting:
    __auth = Variable.get('np_authorization',deserialize_json=True)

    # baseURL='https://hvqlnp01:4993' ='https://10.10.11.11:4993'
    def __init__(self, baseURL=__auth['url']):
        requests.packages.urllib3.disable_warnings()
        self.baseURL = baseURL
        self.api_baseURL = baseURL + '/api/v1'
        self.status_code = None
        self.session = None
        self.connection_name = None
        self.conntction_id = None
        self.connection_status = None
        self.task_id = None
        self.task_name = None
        self.task_enabled = None
        self.task_completed = None
        self.task_status = None

    def __repr__(self):
        return f"<{self.api_baseURL} NPrinting object for API connection>"

    def __del__(self):
        print("NPrinting instance is deleted!")

    def set_baseURL(self, baseURL):
        self.baseURL = baseURL
        self.api_baseURL = baseURL + '/api/v1'

    def connect(self, userid=__auth['userid'], password=__auth['password']):
        # Set up the maximum attempt number and attemp interval of API connection
        retry = Retry(connect=5, backoff_factor=0.5)
        adapter = HTTPAdapter(
            max_retries=retry, pool_connections=50, pool_maxsize=50)
        # Set up a session for the API connection
        s = requests.Session()
        # Use adapter for all requests to endpoints that start with baseURL
        s.mount(self.baseURL, adapter)
        # set up user credentials
        user_auth = HttpNtlmAuth(userid, password)
        # set up connection to the NPrinting server
        r = s.get(self.api_baseURL+'/login/ntlm',
                  auth=user_auth, verify=False, timeout=(10, 30))
        cookies = requests.utils.dict_from_cookiejar(s.cookies)
        token = cookies['NPWEBCONSOLE_XSRF-TOKEN']
        s.headers.update({"Upgrade-Insecure-Requests": "1", "Content-Type": "application/x-www-form-urlencoded",
                         "withCredentials": "True", "X-XSRF-TOKEN": token})
        self.status_code = 200
        self.session = s
        return self

    def get_connection_status(self, connId):
        conn = self.session.get(
            self.api_baseURL+f'/connections/{connId}', verify=False, timeout=(3, 5))
        if conn.status_code == 200:
            return conn.json()['data']
        else:
            raise ConnectionError(
                f'Connection attempt with the connection ID {connId} to check the metadata status failed.')

    def get_task_info(self, taskId):
        task_info = self.session.get(
            self.api_baseURL+f'/tasks/{taskId}', verify=False, timeout=(3, 5))
        if task_info.status_code == 200:
            return task_info.json()['data']
        else:
            raise ConnectionError(
                f'Request to check the information of the task with task ID {taskId} failed.')

    def reload_meta(self, connId):
        self.connection_id = connId
        # reload the metadata connection
        connection_status = self.get_connection_status(connId)['cacheStatus']
        if connection_status in ['Enqueued', 'Generating']:
            pass
        else:
            meta = self.session.post(
                self.api_baseURL + f'/connections/{connId}/reload', timeout=(3, 5))
            if meta.status_code != 200:
                raise ConnectionError(
                    f'Connection attempt with the connection ID {connId} to reload metadata failed.')

        start_time = time.time()
        interval = 5
        timeout = 5 * 60
        check_time = start_time + interval

        while True:
            time.sleep(1)
            current_time = time.time()
            if current_time - start_time > timeout:
                raise TimeoutError(
                    f'Timed out after {timeout} seconds while waiting the metadata reload for "{self.connection_name}" to complete.')
            else:
                if int(current_time) >= int(check_time):
                    conn_data = self.get_connection_status(connId)
                    self.connection_name = conn_data['name']
                    self.connection_status = conn_data['cacheStatus']
                    # Enqueued/Generating/Generated/Aborted/Failed
                    if self.connection_status == 'Generated':
                        # print(f'The metadata for "{self.connection_name}" is reloaded successfully!')
                        return True
                        break
                    elif self.connection_status == 'Aborted' or self.connection_status == 'Failed':
                        raise Exception(
                            f'Reload metadata is {self.connection_status}!')
                    check_time = time.time() + interval

    def execute_task(self, taskId):
        self.task_id = taskId
        task_info = self.session.get(
            self.api_baseURL+f'/tasks/{taskId}', timeout=(3, 5))
        if task_info.status_code == 200:
            self.task_name = task_info.json()['data']['name']
            self.task_enabled = task_info.json()['data']['enabled']
        # execute the task
        task = self.session.post(
            self.api_baseURL+f'/tasks/{taskId}/executions', verify=False, timeout=(3, 5))
        if task.status_code == 202:
            execId = task.json()['data']['id']
            start_time = time.time()
            interval = 10
            timeout = 8 * 3600
            i = 0
            check_time = start_time + interval

            while True:
                time.sleep(1)
                current_time = time.time()
                if current_time - start_time > timeout:
                    raise TimeoutError(
                        f'Timed out after {timeout} s while waiting the task "{self.task_name}" to complete.')
                else:
                    if int(current_time) >= int(check_time):
                        task_status = self.session.get(
                            self.api_baseURL+f'/tasks/{taskId}/executions/{execId}', verify=False, timeout=(3, 5))
                        if task_status.status_code == 200:
                            self.task_status = task_status.json()[
                                'data']['status']
                            task_completed = task_status.json()[
                                'data']['completed']
                            d = datetime.fromtimestamp(time.time()).strftime(
                                '%Y-%m-%d %H:%M:%S')   # Check datetime - for testing only
                            # For testing only
                            # print(f'The status of task "{self.task_name}" is {self.task_status}, and checked at {d} with check number {i}. Current time is {current_time} and Check time is {check_time}.')
                            # None or UTC datetime when completed (ex.2021-11-05T01:52:59.336405Z)
                            if task_completed is not None:
                                self.task_completed = parser.parse(task_completed).astimezone(est).strftime('%Y-%m-%d %H:%M:%S')
                                # print(f'The task "{self.task_name}" is completed on {self.task_completed}.')
                                # Running/Aborted/Completed/CompletedwithWarning
                                return self.task_status
                                break
                            i = i + 1
                            check_time = time.time() + interval*(1 + i/60)
                        else:
                            raise ConnectionError(
                                f'Request to check the status of the task with task ID {taskId} failed.')
        else:
            raise ConnectionError(
                f'Request to execute the task with the task ID {taskId} failed.')
