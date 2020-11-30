import statistics
import datetime
import time
import matplotlib.pyplot as plt

exec_time_list = []
execution_time = dict()
start_time = dict()
end_time = dict()
for i in range(1,4):
    file1 = open(r"log/worker_"+str(i)+".txt", "r")
    lines = file1.readlines()
    for line in lines:
        tp_split = line.split("\t") #tp_split stands for TIMESTAMP_PROGRESS_SPLIT
        #print(tp_split)
        timestamp_string = tp_split[0][0:-1]
        dt = datetime.datetime.strptime(timestamp_string, '%Y-%m-%d %H:%M:%S.%f')
        #print(dt)
        #print(tp_split[1])
        
        mt_split = tp_split[1].split("=")       #mt_split stands for MESSAGE_TASK_SPLIT
        mt_split[0] = mt_split[0].strip()
        mt_split[1] = mt_split[1][0:-1].strip()
        mt_split[1] = mt_split[1][1:]
        #print(mt_split)

        #GETTING TASK_ID
        if ((mt_split[0] == "RECEIVED TASK") or (mt_split[0] == "FINISHED EXECUTING TASK")):
            jt_split = mt_split[1].split(',')           #jt_split stands for JOB_TASK_SPLIT
            ti_split = jt_split[1].split(':')       #ti_split stands for TASK_ID_SPLIT
            task_id = ti_split[1].split(']')[0]
            #print(task_id)
            if (mt_split[0] == "RECEIVED TASK"):
                receive_time = dt
                #print(receive_time)
                start_time[task_id] = receive_time
            if (mt_split[0] == "FINISHED EXECUTING TASK"):
                finish_time = dt
                #print(finish_time)  
                end_time[task_id] = finish_time
            if ((task_id in start_time) and (task_id in end_time)):
                execution_time[task_id] = end_time[task_id] - start_time[task_id]
                #print(task_id, execution_time[task_id].total_seconds())
                exec_time_list.append(execution_time[task_id].total_seconds())
    file1.close()
                
print("Mean execution time of tasks:", statistics.mean(exec_time_list), 's') 
print("Median execution time of tasks:", statistics.median(exec_time_list), 's') 

incoming_time = {}
completion_time = {}
job_execution_time = {}
job_execution_time_list = []
#calculate job completion time          
with open("log/master.txt", 'r') as f:
    lines = f.readlines()
    for line in lines:
        tp_split = line.split("\t") #tp_split stands for TIMESTAMP_PROGRESS_SPLIT
        #print(tp_split)
        timestamp_string = tp_split[0][0:-1]
        dt = datetime.datetime.strptime(timestamp_string, '%Y-%m-%d %H:%M:%S.%f')
        #print(dt)
        
        mj_split = tp_split[1].split("=")       #mj_split stands for MESSAGE_JOB_SPLIT
        mj_split[0] = mj_split[0].strip()
        mj_split[1] = mj_split[1][0:-1].strip()
        mj_split[1] = mj_split[1][1:]
        #print(mt_split)
        
        if ((mj_split[0] == "RECEIVED JOB REQUEST") or (mj_split[0] == "FINISHED EXECUTING JOB")):
            #print(task_id)
            if (mj_split[0] == "RECEIVED JOB REQUEST"):
                jt_split = mj_split[1].split(',')           #jt_split stands for JOB_TASK_SPLIT
                ji_split = jt_split[0].split(':')       #JOB_split stands for JOB_ID_SPLIT
                job_id = ji_split[1]
                receive_time = dt
                #print(receive_time)
                incoming_time[job_id] = receive_time
            if (mj_split[0] == "FINISHED EXECUTING JOB"):
                jt_split = mj_split           #jt_split stands for JOB_TASK_SPLIT
                ji_split = jt_split[1].split(':')       #JOB_split stands for JOB_ID_SPLIT
                job_id = ji_split[1].split(']')[0]
                finish_time = dt
                #print(finish_time)  
                completion_time[job_id] = finish_time
            if ((job_id in incoming_time) and (job_id in completion_time)):
                job_execution_time[job_id] = completion_time[job_id] - incoming_time[job_id]
                job_execution_time_list.append(job_execution_time[job_id].total_seconds())
                #print(job_id, job_execution_time[job_id].total_seconds())
                
print("Mean execution time of jobs:", statistics.mean(job_execution_time_list), 's') 
print("Median execution time of jobs:", statistics.median(job_execution_time_list), 's')

#plot graphs
number_tasks = {1:0, 2:0, 3:0}
x_axis = {1:[], 2:[], 3:[]}
y_axis = {1:[], 2:[], 3:[]}
with open("log/master.txt", 'r') as f:
    lines = f.readlines()
    
    #get time when master started
    first_line = lines[0]
    tp_split = first_line.split("\t") #tp_split stands for TIMESTAMP_PROGRESS_SPLIT
    timestamp_string = tp_split[0][0:-1]
    dt = datetime.datetime.strptime(timestamp_string, '%Y-%m-%d %H:%M:%S.%f')
    start_time = dt
    for worker_id in x_axis:
        x_axis[worker_id].append(0)
        y_axis[worker_id].append(0)
    
    for line in lines:
        if line.strip() != '':
            tp_split = line.split("\t") #tp_split stands for TIMESTAMP_PROGRESS_SPLIT
            #print(tp_split)
            timestamp_string = tp_split[0][0:-1]
            #print(timestamp_string)
            dt = datetime.datetime.strptime(timestamp_string, '%Y-%m-%d %H:%M:%S.%f')
            #print(dt)

            mt_split = tp_split[1].split("=")       #mt_split stands for MESSAGE_TASK_SPLIT
            mt_split[0] = mt_split[0].strip()
            mt_split[1] = mt_split[1][0:-1].strip()
            mt_split[1] = mt_split[1][1:]
            #print(mt_split)


            #GETTING TASK_ID
            if ((mt_split[0] == "SENT TASK TO WORKER") or (mt_split[0] == "UPDATE RECEIVED FROM WORKER")):
                jt_split = mt_split[1].split(',')           #jt_split stands for JOB_TASK_SPLIT
                wi_split = jt_split[0].split(':')       #ti_split stands for WORKER_ID_SPLIT
                worker_id = int(wi_split[1])
                #print(task_id)
                if (mt_split[0] == "SENT TASK TO WORKER"):
                    number_tasks[worker_id] += 1
                    x_axis[worker_id].append((dt - start_time).total_seconds())
                    y_axis[worker_id].append(number_tasks[worker_id])
                if (mt_split[0] == "UPDATE RECEIVED FROM WORKER"):
                    number_tasks[worker_id] -= 1
                    x_axis[worker_id].append((dt - start_time).total_seconds())
                    y_axis[worker_id].append(number_tasks[worker_id])
#print(x_axis)
#print(y_axis)
fig, ax = plt.subplots(figsize=(25, 5))
ax.plot(x_axis[1], y_axis[1], 'b', label="Worker 1")
ax.plot(x_axis[2], y_axis[2], 'g', label="Worker 2")
ax.plot(x_axis[3], y_axis[3], 'r', label="Worker 3")
ax.set_xlabel('Time (s)')
ax.set_ylabel('No of tasks')
ax.set_title('Least Loaded Scheduling (20 jobs)')
legend = ax.legend(loc='upper right', shadow=True, fontsize='x-large')
plt.show()
