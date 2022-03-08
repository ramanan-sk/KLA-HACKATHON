import yaml
import time
import threading
from datetime import datetime

class Solution:
    def __init__(self,workFlow):
        self.startTime=None
        self.endTime=None
        self.workFlow=workFlow
        self.lock = threading.Lock()

    def TimeFunction(self,input):
        executionTime = int(input['ExecutionTime'])
        time.sleep(executionTime)
        #print('sleep :',executionTime)

    def logEntry(self,path,status,function=None,input=None):
        self.lock.acquire()
        now = datetime.now()
        currentTime = now.strftime("%Y-%m-%d %H:%M:%S.%f")

        with open('Milestone1B_Log.txt', 'a') as log:
            if status == 'Entry' or status == 'Exit':
                log.write(f'{currentTime};{path} {status}\n')
            elif status == 'Executing':
                input = '(' + ", ".join(input) + ')'
                log.write(f'{currentTime};{path} {status} {function} {input}\n')
        self.lock.release()

    def executeTask(self,task,execution,path):
        self.logEntry(path,'Entry')
        name = path.split('.')[-1]
        #print(name,path)
        input = task['Inputs']
        if task['Function'] == 'TimeFunction':
            self.logEntry(path,'Executing',task['Function'],list(task['Inputs'].values()))
            self.TimeFunction(input)

        self.logEntry(path,'Exit')

    def executeFlow(self,flow,path):
        self.logEntry(path,'Entry')
        name = path.split('.')[-1]
        execution = flow['Execution']
        activities = flow['Activities']
        if execution == 'Sequential':
            for key in activities.keys():
                if activities[key]['Type'] == 'Task':
                    self.executeTask(activities[key],execution,f'{path}.{key}')
                elif activities[key]['Type'] == 'Flow':
                    self.executeFlow(activities[key],f'{path}.{key}')
            self.logEntry(path,'Exit')
        else:
            threads=[]
            for key in activities.keys():
                if activities[key]['Type'] == 'Task':
                    t = threading.Thread(target=self.executeTask,args=(activities[key],execution,f'{path}.{key}'))
                    threads.append(t)
                    #self.executeTask(activities[key],execution,f'{path}.{key}')
                elif activities[key]['Type'] == 'Flow':
                    t = threading.Thread(target=self.executeFlow,args=(activities[key],f'{path}.{key}'))
                    threads.append(t)
                    # self.executeFlow(activities[key],f'{path}.{key}')
                t.start()
            for x in threads:
                x.join()
            self.logEntry(path,'Exit')

    def start(self):
        workFlowName = list(self.workFlow.keys())[0]
        for flow in self.workFlow.keys():
            self.executeFlow(self.workFlow[flow],flow)

with open('./DataSet/Milestone1/Milestone1B.yaml') as dataset:
#with open('./DataSet/Examples/Milestone1/Milestone1_Example.yaml') as dataset:
    workFlow = yaml.safe_load(dataset)
    solution = Solution(workFlow)
    solution.start()
    dataset.close()


