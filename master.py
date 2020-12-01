
import socket
from _thread import *
import threading
import json
import sys
import random
from datetime import *
import operator
import os

class myThread(threading.Thread):
    def __init__(self, threadID, func):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.func = func
    
    def run(self):
        #print ("Starting ", self.threadID)
        self.func()
        #print ("Exiting " , self.threadID)
 
def randomScheduling():
    while True:
        worker_id = random.randint(1, 3)
        if availableSlots[worker_id]['slots'] > 0:
                return worker_id

def roundRobinScheduling():
    while True:
        for worker_id in range(1,4):
            if availableSlots[worker_id]['slots'] > 0:
                    return worker_id
                
def leastLoadedScheduling():
    res_worker_id = 1
    for worker_id in range(2, 4):
        if availableSlots[worker_id]['slots'] > availableSlots[res_worker_id]['slots']:
            res_worker_id = worker_id
    return res_worker_id

#SENDS THE TASK TO THE APPROPRIATE WORKER      
def scheduler():
    global taskQueue
    scheduling_algo = sys.argv[2]
    while(True):
        if(len(taskQueue) > 0):
            for task in taskQueue:
                if scheduling_algo == "RANDOM": 
                    worker_id = randomScheduling()
                elif scheduling_algo == "RR":
                    worker_id = roundRobinScheduling()
                elif scheduling_algo ==  "LL":
                    worker_id = leastLoadedScheduling()
                else:
                    print("Enter either RANDOM or RR or LL as the second argument")
                    sys.exit()
                port = availableSlots[worker_id]['port']
                
                task = taskQueue[0]
                taskQueue.remove(task)
                task_message = json.dumps(task)
                #print("Sending task", task['task_id'], "to worker", worker_id, "on port", port)
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                    s.connect(("localhost", port))
                    printLock.acquire()
                    with open("log/master.txt", "a+") as f:
                        f.write(str(datetime.now()) + "\tOUTGOING CONNECTION ESTABLISHED = [host:{0}, port:{1}]\n".format("localhost", port))
                    printLock.release()
                    s.send(task_message.encode())
                printLock.acquire()
                with open("log/master.txt", "a+") as f:
                    f.write(str(datetime.now()) + "\tSENT TASK TO WORKER= [worker_id:{0}, task_id:{1}, job_id:{2}, duration:{3}]\n".format(\
                            worker_id, task['task_id'], task['job_id'], task['duration']))
                printLock.release()
                print("Sent task", task['task_id'], "to worker", worker_id, "on port", port)
                
                workerLock.acquire()
                availableSlots[worker_id]['slots'] -= 1
                workerLock.release()
    
       
#listen for requests
def getRequests():
    host = ""
    port = 5000
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    
    global mapTaskCounts
    global taskQueue
    global reduceTasks
    global jobTaskCounts
    
    with s:
        s.bind((host, port))
        s.listen(1000)
        while(True):
            c, addr = s.accept()
            printLock.acquire()
            with open("log/master.txt", "a+") as f:                
                f.write(str(datetime.now()) + "\tINCOMING CONNECTION ESTABLISHED = [host:{0}, port:{1}]\n".format(addr[0], addr[1]))
            printLock.release()
            with c:
                req = c.recv(100000)
                req = json.loads(req)
                mapTaskIds = [i['task_id'] for i in req['map_tasks']]
                reduceTaskIds = [i['task_id'] for i in req['reduce_tasks']]
                printLock.acquire()
                with open("log/master.txt", "a+") as f:                    
                    f.write(str(datetime.now()) + "\tRECEIVED JOB REQUEST = [job_id:{0}, map_tasks_ids:{1}, reduce_tasks_ids:{2}]\n".format(\
                            req['job_id'], mapTaskIds, reduceTaskIds))
                printLock.release()
                mapTaskCounts[req['job_id']] = {}
                no_map_tasks = len(req['map_tasks'])
                mapTaskCounts[req['job_id']] = no_map_tasks #number of map tasks
                #print(mapTaskCounts)
                
                jobTaskCounts[req['job_id']] = len(req['map_tasks']) + len(req['reduce_tasks'])
                
                #store reduce tasks to schedule them after all the map tasks finish execution
                reduceTasks[req['job_id']] = []
                tasks = req['reduce_tasks']
                task = {}
                for i in tasks:
                    task[i['task_id']] = {}
                    task[i['task_id']]['task_id'] = i['task_id'] 
                    task[i['task_id']]['job_id'] = req['job_id']
                    task[i['task_id']]['duration'] = i['duration']
                    reduceTasks[req['job_id']].append(task[i['task_id']])
                #print(reduceTasks)
                
                #store map tasks 
                tasks = req['map_tasks']
                task = {}
                for i in tasks:
                    task[i['task_id']] = {}
                    task[i['task_id']]['task_id'] = i['task_id'] 
                    task[i['task_id']]['job_id'] = req['job_id']
                    task[i['task_id']]['duration'] = i['duration']
                    taskQueue.append(task[i['task_id']])
                

def getUpdatesWorkers():
    global availableSlots
    global mapTaskCounts
    global jobTaskCounts
    
    host = ""
    port = 5001
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    with s:
        s.bind((host, port))
        s.listen(1000)
        while(True):
            c, addr = s.accept()
            printLock.acquire()
            with open("log/master.txt", "a+") as f:
                f.write(str(datetime.now()) + "\tINCOMING CONNECTION ESTABLISHED = [host:{0}, port:{1}]\n".format(addr[0], addr[1]))
            printLock.release()
            with c:
                update_json = c.recv(1000)
                update = json.loads(update_json)
                worker_id = update['worker_id']
                job_id = update['job_id']
                task_id = update['task_id']
                printLock.acquire()
                with open("log/master.txt", "a+") as f:
                    f.write(str(datetime.now()) + "\tUPDATE RECEIVED FROM WORKER = [worker_id:{0}, task_id:{1}] TASK COMPLETED\n"\
                            .format(worker_id, task_id))
                printLock.release()
                print("Task", task_id, "completed execution in", worker_id)
                jobTaskCounts[job_id] -= 1
                if jobTaskCounts[job_id] == 0:
                    printLock.acquire()
                    with open("log/master.txt", "a+") as f:
                        f.write(str(datetime.now()) + "\tFINISHED EXECUTING JOB = [job_id:{0}]\n".format(job_id))
                    printLock.release()
                workerLock.acquire()
                availableSlots[worker_id]['slots'] += 1
                workerLock.release()
                
                if 'M' in task_id: #map job is completed
                    mapTaskCounts[job_id] -= 1
                    if mapTaskCounts[job_id] == 0:
                        for reduce_task in reduceTasks[job_id]:
                            taskQueue.append(reduce_task)
                        
                    
availableSlots = {} #number of free slots of every worker
mapTaskCounts = {} #number of map tasks of every job remaining to be executed
jobTaskCounts = {} #total number of tasks in every job remaining to be executed
reduceTasks = {} #list of reduce tasks for every job
taskQueue = [] #list of tasks to be scheduled
workerLock = threading.Lock()
taskLock = threading.Lock()
printLock = threading.Lock()


def main():
    try:
        os.mkdir('log')
    except:
        pass #dir already exists
    #delete any previous log file
    f = open("log/master.txt", "w")
    f.close()
    path_to_config = sys.argv[1]
    with open(path_to_config) as f:
        config = json.load(f)
    #print(config)
    global availableSlots
    for worker in config['workers']:
        availableSlots[worker['worker_id']] = {'slots':worker['slots'], 'port':worker['port']}
    print(availableSlots)
    thread1 = myThread(1, getRequests)
    thread2 = myThread(2, getUpdatesWorkers)
    thread3 = myThread(3, scheduler)
    thread1.start()
    thread2.start()
    thread3.start()
    thread1.join()
    thread2.join()
    thread3.join()
    
if __name__ == '__main__':
    main()    
