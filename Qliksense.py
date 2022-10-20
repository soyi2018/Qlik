#!/usr/bin/env python
# coding: utf-8

# Qliksense API v2
import sys
import time
import json
import requests
import pytz
from datetime import datetime
from dateutil import parser
from requests.adapters import HTTPAdapter, Retry
from requests.exceptions import ConnectionError
from requests_ntlm import HttpNtlmAuth

from airflow.models import Variable

est = pytz.timezone('US/Eastern')
xrf = 'iX83QmNlvu87yyAB'
# set up necessary headers
headers = {'X-Qlik-xrfkey': xrf,
           "Content-Type": "application/json",
           "User-Agent": "Windows"}

class Qliksense:
    __auth = Variable.get('qs_authorization',deserialize_json=True)

    def __init__(self, baseURL=__auth['url']):
        requests.packages.urllib3.disable_warnings()
        self.baseURL = baseURL
        self.api_baseURL = baseURL + '/qrs{}?xrfkey={}'
        self.user_auth = None
        self.status_code = None
        self.session = None
        self.app_id = None
        self.app_name = None
        self.app_lastReload = None
        self.task_id = None
        self.task_name = None
        self.task_enabled = None
        self.task_completed = None
        self.task_status = None

    def __repr__(self):
        return f"<{self.api_baseURL} Qliksense object for API connection>"

    def __del__(self):
        return f"Qliksense instance is deleted!"

    def set_baseURL(self, baseURL):
        self.baseURL = baseURL
        self.api_baseURL = baseURL + '/qrs{}?xrfkey={}'

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
        self.user_auth = HttpNtlmAuth(userid, password)
        # check the health of Qliksense server
        check_items = {'Qlik Repository': '/qrs/about',
                       'Qlik Engine': '/engine/healthcheck', 'Qlik Printing': '/printing/alive'}
        for item in check_items.items():
            r = s.get(self.baseURL+f'{item[1]}?xrfkey={xrf}', headers=headers,
                      auth=self.user_auth, verify=False, timeout=(5, 10))
            if r.status_code != 200:
                raise SystemError(f'{item[0]} is not running properly!')
        self.status_code = 200
        self.session = s
        return self

    def reload_app(self, appId, timeout=21600):
        '''A task with the name "Manually triggered reload of ..." will be created automatically in QMC if not exists, otherwise
        the existing "Manually triggered reload of ..." task will be started'''
        self.app_id = appId
        # get app information
        app_info = self.get_app_info(appId)
        self.app_name = app_info['name']
        lastRT = parser.parse(app_info['lastReloadTime']).astimezone(est)
        # reload data in the app
        reload = self.session.post(self.api_baseURL.format(
            f'/app/{appId}/reload', xrf), headers=headers, auth=self.user_auth, verify=False, timeout=(3, 5))
        if reload.status_code == 204:
            start_time = time.time()
            interval = 10
            # timeout = 6 * 3600
            i = 0
            check_time = start_time + interval

            while True:
                time.sleep(1)
                current_time = time.time()
                if current_time - start_time > timeout:
                    raise TimeoutError(
                        f'Timed out after {timeout} s while waiting the data reload for the app "{self.app_name}" to complete.')
                else:
                    if int(current_time) >= int(check_time):
                        d = datetime.fromtimestamp(time.time()).strftime(
                            '%Y-%m-%d %H:%M:%S')   # Check datetime - for testing only
                        # For testing only
                        # print(
                        #     f'The status of app "{self.app_name}" is checked at {d} with check number {i}. Current time is {current_time} and Check time is {check_time}.')
                        app_info = self.get_app_info(appId)
                        newRT = parser.parse(
                            app_info['lastReloadTime']).astimezone(est)
                        if newRT > lastRT:
                            # for testing only
                            # print(
                            #     f'The data in the app "{self.app_name}" is reloaded successfully!')
                            self.app_lastReload = newRT.strftime(
                                '%Y-%m-%d %H:%M:%S')
                            return True
                            break
                        i = i + 1
                        check_time = time.time() + interval*(1 + i/60)
        else:
            raise ConnectionError(
                f'Attemp to request the reload for the app "{self.app_name}" with the app ID {appId} failed.')

    def get_task_info(self, taskId):
        t = self.session.get(self.api_baseURL.format(
            f'/reloadtask/{taskId}', xrf), headers=headers, auth=self.user_auth, verify=False, timeout=(3, 5))
        if t.status_code == 200:
            task = dict(name=t.json()['name'], enabled=t.json()[
                        'enabled'], isManuallyTriggered=t.json()['isManuallyTriggered'])
            task_info = dict(app=t.json()['app'], task=task)
            return task_info
        else:
            raise ConnectionError(
                f'Attempt to check the status for the task with task ID {taskId} failed.')

    def get_app_info(self, appId):
        r = self.session.get(self.api_baseURL.format(
            f'/app/{appId}', xrf), headers=headers, auth=self.user_auth, verify=False, timeout=(3, 5))
        if r.status_code == 200:
            return r.json()
        else:
            raise ConnectionError(
                f'Connection attempt to the app with the app ID {appId} failed.')

    def get_active_execution(self, appId):
        url = self.baseURL + \
            f'/qrs/executionresult/full?filter=appId eq {appId}&xrfkey={xrf}'
        exec_result = self.session.get(
            url, headers=headers, auth=self.user_auth, verify=False, timeout=(3, 5))
        if exec_result.status_code == 200:
            for exec in exec_result.json():
                if exec['stopTime'] == '1753-01-01T00:00:00.000Z' and exec['executionID'] != '00000000-0000-0000-0000-000000000000':
                    return exec['executionID'], exec['taskID']
        else:
            raise ConnectionError(
                f'Connection attempt to obtain the active execution for the app "{appId}" failed.')

    def start_task(self, taskId):
        url = self.baseURL + \
            f'/qrs/task/{taskId}/start/synchronous?xrfkey={xrf}'
        t = self.session.post(url, headers=headers,
                              auth=self.user_auth, verify=False, timeout=(3, 5))
        execId = t.json()['value']
        if t.status_code == 201 and execId != '00000000-0000-0000-0000-000000000000':
            return execId
        else:
            raise ConnectionError(
                f'Attempt to start the task with task ID {taskId} failed. Bad connection, incorrect task ID or another running task for the same app.')

    def stop_task(self, taskId):
        url = self.api_baseURL.format(f'/task/{taskId}/stop', xrf)
        t = self.session.post(url, headers=headers, auth=self.user_auth, verify=False, timeout=(3, 5))
        time.sleep(5)
        if t.status_code == 204:
            app = self.get_task_info(taskId)['app']
            appId = app['id']
            appName = app['name']
            active_exec = self.get_active_execution(appId)
            if not active_exec:
                return True
            else:
                raise RuntimeError(
                    f'Attempt to stop the task with task ID {taskId} failed. Another task with task ID {active_exec[1]} for the app {appName} is still running. Please stop the active task manually in QMC.')
        else:
            raise ConnectionError(
                f'Attempt to stop the task with task ID {taskId} failed. Please stop the task manually in QMC.')

    def loop_execution_status(self, execId, timeout=36000):
        url = self.baseURL + \
            f'/qrs/executionresult/full?filter=ExecutionId eq {execId}&xrfkey={xrf}'
        all_status = {0: "NeverStarted", 1: "Triggered", 2: "Started", 3: "Queued", 4: "AbortInitiated", 5: "Aborting", 6: "Aborted",
                      7: "FinishedSuccess", 8: "FinishedFail", 9: "Skipped", 10: "Retry", 11: "Error", 12: "Reset", }
        exec_info = self.session.get(
            url, headers=headers, auth=self.user_auth, verify=False, timeout=(3, 5))
        if exec_info.status_code == 200:
            taskId = exec_info.json()[0]['taskID']
            task_info = self.get_task_info(taskId)
            appName = task_info['app']['name']
        else:
            raise ConnectionError(
                f'Request to check the status of task execution "{execId}" failed.')

        start_time = time.time()
        interval = 10
        # timeout = 10 * 3600
        i = 0
        check_time = start_time + interval

        while True:
            time.sleep(1)
            current_time = time.time()
            if current_time - start_time > timeout:
                if self.stop_task(taskId):
                    raise TimeoutError(f'Timed out after {timeout}s while waiting the task {taskId} for the app "{appName}" to complete. The task {taskId} has been stopped.')
            else:
                if int(current_time) >= int(check_time):
                    exec_info = self.session.get(
                        url, headers=headers, auth=self.user_auth, verify=False, timeout=(3, 5))
                    if exec_info.status_code == 200:
                        startTime = exec_info.json()[0]['startTime']
                        stopTime = exec_info.json()[0]['stopTime']
                        statusId = exec_info.json()[0]['status']
                        status = all_status[statusId]
                        d = datetime.fromtimestamp(time.time()).strftime(
                            '%Y-%m-%d %H:%M:%S')   # Check datetime - for testing only
                        # For testing only
                        # print(
                        #     f'The status of task {taskId} with execution {execId} for the app "{appName}" is {status}, and checked at {d} with check number {i}. Current time is {current_time} and Check time is {check_time}.')
                        if stopTime != '1753-01-01T00:00:00.000Z' and statusId == 7:
                            appId = exec_info.json()[0]['appID']
                            app_info = self.get_app_info(appId)
                            self.app_lastReload = parser.parse(app_info['lastReloadTime']).astimezone(
                                est).strftime('%Y-%m-%d %H:%M:%S')
                            # For testing only
                            # print(
                            #     f'The task {taskId} with execution {execId} for the app "{appName}" is completed on {self.app_lastReload}!')
                            return status
                            break
                        else:
                            if stopTime != '1753-01-01T00:00:00.000Z' and statusId != 7:
                                raise ChildProcessError(
                                    f'The task {taskId} with execution {execId} for the app "{appName}" is completed but with errors. The status of the task execution is {status}.')
                        i = i + 1
                        check_time = time.time() + interval*(1 + i/60)
                    else:
                        raise ConnectionError(
                            f'Request to check the status of task execution "{execId}" failed.')

    # action=proceed/skip/wait/stop/error
    def execute_task(self, taskId, action='proceed',timeout=36000):
        self.task_id = taskId
        task_info = self.get_task_info(taskId)
        self.task_name = task_info['task']['name']
        self.task_enabled = task_info['task']['enabled']
        self.app_id = task_info['app']['id']
        self.app_name = task_info['app']['name']
        options = ['proceed', 'skip', 'wait', 'stop', 'error']

        # Check if task enabled
        if task_info['task']['enabled']:
            # Check if there is active execution
            active_exec = self.get_active_execution(self.app_id)
            if active_exec:
                # Proceed with the active running task by monitoring its status
                if action == 'proceed' or action not in options:
                    status = self.loop_execution_status(active_exec[0],timeout)
                    status = f'{status} with the active execution {active_exec[0]} for the task {active_exec[1]} instead'
                # Skip the active running task without monitoring its status
                if action == 'skip':
                    status = f'Skipped with another task {active_exec[1]} running'
                # Wait to execute the new task until the active running task is completed
                if action == 'wait':
                    active_execId = active_exec[0]
                    active_taskId = active_exec[1]
                    if self.loop_execuion_status(active_execId):
                        execId = self.start_task(taskId)
                        status = self.loop_execution_status(execId,timeout)
                        status = f'{status} after another running task {active_taskId} is completed'
                    else:
                        raise RuntimeError(
                            f'Attempt to exec the task with task ID {taskId} failed. Another task with task ID {active_taskId} for the app {self.app_name} is still running.')
                # Stop the active running task and then execute the new task
                if action == 'stop':
                    active_execId = active_exec[0]
                    active_taskId = active_exec[1]
                    if self.stop_task(active_taskId):
                        execId = self.start_task(taskId)
                        status = self.loop_execution_status(execId,timeout)
                        status = f'{status} by stopping another running task {active_taskId}'
                    else:
                        raise RuntimeError(
                            f'Attempt to stop the active task with task ID {active_taskId} for the app {self.app_name} failed.')
                # Raise error if there is an active running task
                if action == 'error':                                   
                    active_taskId = active_exec[1]
                    raise RuntimeError(
                        f'Attempt to exec the task with task ID {taskId} failed. Another task with task ID {active_taskId} for the app {self.app_name} is still running.')
            else:
                execId = self.start_task(taskId)
                status = self.loop_execution_status(execId,timeout)
            return status
        else:
            raise PermissionError(
                f'{self.task_name} with task ID {taskId} is not enabled to execute!')
