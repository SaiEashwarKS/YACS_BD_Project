# YACS
YACS stands for Yet Another Centralized Scheduler. This is used to schedule map and reduce tasks on several workers. Every worker has some number of slots (compute). Each slot can execute one task. The number of slots in different workers can be different. The configuration of each worker is sent as json to the master via config.json. 

master.py : consists of 3 threads
![master_threads](https://github.com/SaiEashwarKS/YACS_BD_Project/blob/main/MasterFlowchart.png)

worker.py : consists of 2 threads
![worker_threads](https://github.com/SaiEashwarKS/YACS_BD_Project/blob/main/WorkerFlowchart.png)

requests.py : generate n number of requests with random number of map and reduce tasks for every job.
Format of request message:
```json
{	"job_id":<job_id>,	
	"map_tasks":[
		{"task_id":"<task_id>","duration":<in seconds>},	
		{"task_id":"<task_id>","duration":<in seconds>}	
		...	
	],	
	"reduce_tasks":[	
		{"task_id":"<task_id>","duration":<in seconds>},	
		{"task_id":"<task_id>","duration":<in seconds>}
		...	
	]
}
```

Format of config.json :
```json
{
	 "Workers": [ //one worker per machine
	    {
		      "worker_id": <worker_id>,
		      "slots": <number of slots>, // number of slots in the machine
		      "port": <port number> // port on which the Worker process listens for task launch messages
	    },
	    {
		      ”worker_id": <worker_id>,
		      "slots": <number of slots>,
		      "port": <port number>
	    },
   	   …
	]
}

```
YACS must follow the following Map-Reduce task dependancy
- There is no priority among map tasks
- There is no priority among reduce tasks
- Reduce tasks can be scheduled only after all the map tasks have completed their execution

There are three scheduling algorithms:
- Round Robin (RR)
- Random (RANDOM)
- Least Loaded (LL)



# Steps to execute YACS
modules that will be imported:
>numpy
random
matplotlib
threading
json
time
datetime
socket
statistics

Note : Create an empty directory named 'log' when executing YACS for the first time

- run master.py 
```sh 
        python master.py /path/to/config.json scheduling_algo(RR/RANDOM/LL) 
```
- run worker.py in as many consoles as there are workers
```sh
        python worker.py <port> <worker_id>
```
- run requests.py with the number of requests needed
```sh
        python requests.py <no_of_requests>
```


After executing the YACS at least once (and without deleting the log files), running stats.py will calculate
- the mean and median time of execution of all tasks (in seconds)
- the mean and median time of execution of all jobs (in seconds)
- plot graph of number of tasks on each worker against worker for all the 3 scheduling algorithms

# Graph of number of tasks on each worker against worker for all the 3 scheduling algorithms for a request of 20 jobs with random.seed of 1 in requests.py
![graph for Round Robin Scheduling](https://github.com/SaiEashwarKS/YACS_BD_Project/blob/main/RR_graph.png)
![graph for Random Scheduling](https://github.com/SaiEashwarKS/YACS_BD_Project/blob/main/RANDOM_graph.png)
![graph for Least Loaded Scheduling](https://github.com/SaiEashwarKS/YACS_BD_Project/blob/main/LL_graph.png)

