# -*- coding: utf-8 -*-
"""
Created on Wed Nov 25 17:17:22 2020

@author: HP
"""

import sys
import threading
import json
from socket import *
import time
import random
import numpy as np
from time import *
from datetime import *
import os

w_id = sys.argv[2]
#f = open("log/worker_"+w_id+".txt", "w")    #LOG FILE

#CLASS FOR TASK
class Task:
    def __init__(self, job_id, task_id, time_left):
        self.job_id = job_id
        self.task_id = task_id
        self.time_left = time_left
    
    #REDUCES THE TIME BY 1 SECOND
    def reduce(self):
        self.time_left -= 1
        
    #CHECKS IF THERE IS ANY TIME REMAINING IN THE TASK
    def time_check(self):
        time_left = self.time_left
        return (time_left == 0)

#CLASS FOR WORKER
class Worker:
    def __init__(self, worker_id, port):
        self.port = port
        self.worker_id = worker_id
        self.slots = 0
        self.free_slots = 0
        self.exec_pool = []
        #WORKER ID DETERMINES THE NUMBER OF SLOTS (ACCORDING TO THE CONFIG FILE)
        if worker_id == 1:
            self.slots = 5
            self.free_slots = 5
            self.exec_pool = [0 for _ in range(5)]
                
        elif worker_id == 2:
            self.slots = 7
            self.free_slots = 7
            self.exec_pool = [0 for _ in range(7)]
        
        elif worker_id == 3:
            self.slots = 3
            self.free_slots = 3
            self.exec_pool = [0 for _ in range(3)]
        with open("log/worker_"+w_id+".txt", "a+") as f:
            printLock.acquire()
            f.write(str(datetime.now()) + ":\tWORKER HAS STARTED = [worker_id:{0}, port:{1}, slots:{2}]\n".format(
            self.worker_id,
            self.port,
            self.slots))
            printLock.release()
    
    #CHECKS FOR FREE SLOTS IN THE WORKER
    def free_slots(self):
        free_slots = self.free_slots
        if (free_slots):
            return True
        else:
            return False

    
    #WORKER RECEIVES THE TASK LAUNCH MESSAGE
    #LAUNCHING THE RECEIVED TASK ON A FREE SLOT
    def launch_task(self, task_dict):
        task = Task(task_dict["job_id"], task_dict["task_id"], task_dict["duration"])
        with open("log/worker_"+w_id+".txt", "a+") as f:
            printLock.acquire()
            f.write(str(datetime.now()) + ":\tSTARTED EXECUTING TASK = [job_id:{0}, task_id:{1}]\n".format(
                task_dict["job_id"],
                task_dict["task_id"]))
            printLock.release()
        #workerLock.acquire()
        for i in range(len(self.exec_pool)):
            if (isinstance(self.exec_pool[i], int) and self.exec_pool[i] == 0):
                self.exec_pool[i] = task
                self.free_slots -= 1
                break
        #workerLock.release()
            
    
    #REMOVING A TASK FROM EXECUTION POOL IF TIME_LEFT IS 0
    #FUNCTION IS CALLED EVERY SECOND
    #IT LOOPS THROUGH EXEC_POOL AND REMOVES ANY FINISHED TASKS
    def remove_task(self, exec_pool_index):
        self.exec_pool[exec_pool_index] = 0
        self.free_slots += 1
    
#THREAD C
#THIS FUNCTION LISTEN FOR THE TASK LAUNCH MESSAGE FROM MASTER
#ON RECEIVING THE MESSAGE IT ADDS THE TASK TO THE EXECUTION POOL
def get_task_launch_msg(worker):
    s = socket(AF_INET, SOCK_STREAM)
    with s:
        s.bind(("localhost", worker.port))
        s.listen(1024)
        while True:
            c, addr = s.accept()
            with open("log/worker_"+w_id+".txt", "a+") as f:
                printLock.acquire()
                f.write(str(datetime.now()) + ":\tINCOMING CONNECTION ESTABLISHED = [host:{0}, port:{1}]\n".format(addr[0], addr[1]))
                printLock.release()
            with c:
                task_launch_msg_json = c.recv(1024).decode()
                if task_launch_msg_json:
                    task = json.loads(task_launch_msg_json)
                    with open("log/worker_"+w_id+".txt", "a+") as f:
                        printLock.acquire()
                        f.write(str(datetime.now()) + ":\tRECEIVED TASK = [job_id:{0}, task_id:{1}, duration:{2}]\n".format(
                            task["job_id"], 
                            task["task_id"], 
                            task["duration"]))
                        printLock.release()
                    workerLock.acquire()
                    worker.launch_task(task)
                    workerLock.release()
        
        
#THREAD D
#THIS FUNCTION ITERATES THROUGH THE EXECUTION POOL 
#AND REDUCES THE TIME_LEFT VARIABLE OF EACH TASK EVERY SECOND     
#IT REMOVES A TASK FROM POOL IF TIME_LEFT == 0            
def execution_of_tasks(worker):
    while True:
        if(worker.slots == 0): #worker has not been initialsied yet
            continue
        for i in range(worker.slots):
            if (isinstance(worker.exec_pool[i], int) and worker.exec_pool[i] == 0):
                continue
            elif (worker.exec_pool[i].time_check()):
                task = worker.exec_pool[i]
                job_id = task.job_id
                task_id = task.task_id
                msg = { 
                    "worker_id": worker.worker_id, 
                    "job_id": job_id, 
                    "task_id": task_id
                    }
                with open("log/worker_"+w_id+".txt", "a+") as f:
                    printLock.acquire()
                    f.write(str(datetime.now()) + ":\tFINISHED EXECUTING TASK = [job_id:{0}, task_id:{1}]\n".format(job_id, task_id))
                    printLock.release()
                
                workerLock.acquire()
                worker.remove_task(i)
                workerLock.release()
                
                msg_str = json.dumps(msg)
                s = socket(AF_INET, SOCK_STREAM)
                with s:
                    s.connect(('localhost', 5001))
                    with open("log/worker_"+w_id+".txt", "a+") as f:
                        printLock.acquire()
                        f.write(str(datetime.now()) + ":\tOUTGOING CONNECTION ESTABLISHED = [host:{0}, port:{1}]\n".format("localhost", "5001"))
                        printLock.release()
                    s.send(msg_str.encode())
                with open("log/worker_"+w_id+".txt", "a+") as f:
                    printLock.acquire()
                    f.write(str(datetime.now()) + ":\tUPDATE SENT TO MASTER = [job_id:{0}, task_id:{1}] COMPLETED\n".format(job_id, task_id))
                    printLock.release()
            else:
                workerLock.acquire()
                worker.exec_pool[i].reduce()
                workerLock.release()
        sleep(1)
  
taskLock = threading.Lock()
workerLock = threading.Lock()
printLock = threading.Lock() #to lock printing to log files          

if __name__ == "__main__":
    try:
        os.mkdir('log')
    except:
        pass #dir already exists
    #delete previous log file
    f = open("log/worker_"+w_id+".txt", "w")
    f.close()
    if (len(sys.argv) != 3):
        print("Usage: python Worker.py <port number> <worker id>")
    port = int(sys.argv[1])
    worker_id = int(sys.argv[2])
    worker = Worker(worker_id, port)
    
    t1 = threading.Thread(target = get_task_launch_msg, args = (worker,))
    t2 = threading.Thread(target = execution_of_tasks, args = (worker,))
    t1.start()
    t2.start()
    t1.join()
    t2.join()
