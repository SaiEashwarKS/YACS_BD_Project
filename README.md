# YACS
YACS stands for Yet Another Centralized Scheduler. This is used to schedule map and reduce tasks on several workers. Every worker has some number of slots (compute). Each slot can execute one task. The number of slots in different workers can be different. The configuration of each worker is sent as json to the master via config.json. 

master.py : consists of 3 threads. Master listens to job requests from requests.py from port 5000. Master receives updates from workers from port 5001.
![master_threads](https://github.com/SaiEashwarKS/YACS_BD_Project/blob/main/images/MasterFlowchart.png)

worker.py : consists of 2 threads
![worker_threads](https://github.com/SaiEashwarKS/YACS_BD_Project/blob/main/images/WorkerFlowchart.png)

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

>random

>matplotlib

>threading

>json

>time

>datetime

>socket

>statistics

>os


- run master.py 
```sh 
        python master.py </path/to/config.json> <scheduling_algo(RR/RANDOM/LL)> 
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

# analysis.py
(must be run after executing YACS at least once and without deleting the log files/folder)
```sh
python analysis.py
```
- plots bar graph of executiuon time of tasks
- computes the mean and median of execution times of tasks
- plots bar plot of execution time of jobs
- computes the mean and median of execution times of jobs
- plots a graph of number of tasks on each worker against worker

# Bar plots of task and job execution times for RANDOM scheduling for a request of 5 jobs with random.seed of 1 in requests.py
![task execution times bar plot](https://github.com/SaiEashwarKS/YACS_BD_Project/blob/main/images/TaskExecutionGraph.png)

Mean execution time of tasks: 3.3261716666666667 s

Median execution time of tasks: 3.359166 s

![job execution times bar plot](https://github.com/SaiEashwarKS/YACS_BD_Project/blob/main/images/JobExecutionGraph.png)

Mean execution time of jobs: 8.543104 s

Median execution time of jobs: 8.92703 s

# Graph of number of tasks on each worker against worker for all the 3 scheduling algorithms for a request of 20 jobs with random.seed of 1 in requests.py
![graph for Round Robin Scheduling](https://github.com/SaiEashwarKS/YACS_BD_Project/blob/main/images/RR_graph.png)
![graph for Random Scheduling](https://github.com/SaiEashwarKS/YACS_BD_Project/blob/main/images/RANDOM_graph.png)
![graph for Least Loaded Scheduling](https://github.com/SaiEashwarKS/YACS_BD_Project/blob/main/images/LL_graph.png)

