import re
import pandas as pd
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
        self.data= {}

    def TimeFunction(self,input):
        executionTime = int(input['ExecutionTime'])
        time.sleep(executionTime)

    def DataLoad(self,input,path):
        print('Hi')
        filename = input['Filename']
        dataset=pd.read_csv('/content/'+filename)
        self.data[filename]=dataset
        var = path+'.NoOfDefects'
        var = '_'.join(var.split('.'))
        self.data[var]=len(dataset)
        print(self.data[filename])
        print('len',self.data[var],'  ',var)

    def logEntry(self,path,status,function=None,input=None):
        self.lock.acquire()
        now = datetime.now()
        currentTime = now.strftime("%Y-%m-%d %H:%M:%S.%f")
        
        with open('Milestone2B_Log.txt', 'a') as log:
            if status == 'Entry' or status == 'Exit':
                log.write(f'{currentTime};{path} {status}\n')
            elif status == 'Executing':
                input = list(input.values())
                for i in range(len(input)):
                    input[i]=str(input[i])
                input = ", ".join(input)
                print("INPUT ",input)

                log.write(f'{currentTime};{path} {status} {function} ({input})\n')
            elif status == 'Skipped':
                log.write(f'{currentTime};{path} {status}\n')
        self.lock.release()

    def executeTask(self,task,execution,path):
        self.logEntry(path,'Entry')
        name = path.split('.')[-1]
        input = task['Inputs']
        if 'FunctionInput' in input.keys():
          print('Welcome')
          v = input['FunctionInput']
          if '$' in v:
            v = v[2:-1]
            print(input['FunctionInput'],v)
            var = '_'.join(v.split('.'))
            input['FunctionInput']=self.data[var]
            print('DATA',self.data[var])
        print(input)
        if 'Condition' in task.keys():
            if '$(' in task['Condition']:
                con = task['Condition']
                var = re.findall(r'(?<=[(]).*(?=[)] )',con)
                var = '_'.join(var[0].split('.'))
                while var not in self.data.keys():
                  pass
                if var in self.data.keys():
                    d = self.data[var]
                    con = re.sub(r'[$][(].*[)]',str(d),con)
                    print(con)
                    if eval(con)==False:
                        self.logEntry(path,'Skipped')
                    else:
                        if task['Function'] == 'TimeFunction':
                            self.logEntry(path,'Executing',task['Function'],input)
                            self.TimeFunction(input)
                        elif task['Function'] == 'DataLoad':
                            self.logEntry(path,'Executing',task['Function'],input)
                            self.DataLoad(input,path)
                else:
                  print('No')
        else :
            if task['Function'] == 'TimeFunction':
                self.logEntry(path,'Executing',task['Function'],input)
                self.TimeFunction(input)
            elif task['Function'] == 'DataLoad':
                self.logEntry(path,'Executing',task['Function'],input)
                self.DataLoad(input,path)

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

with open('/content/Milestone2B.yaml') as dataset:
#with open('./DataSet/Examples/Milestone1/Milestone1_Example.yaml') as dataset:
    workFlow = yaml.safe_load(dataset)
    solution = Solution(workFlow)
    solution.start()
    dataset.close()
