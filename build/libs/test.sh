# #!/bin/bash

gradle -p ../.. build

# # Clean up HDFS directories (ignore errors if they don't exist)
# Q1
hdfs dfs -rm -r /ms/q1/job_one 
hdfs dfs -rm -r /ms/q1/job_two 
hdfs dfs -rm -r /ms/q1/final_answer 

#Q2
hdfs dfs -rm -r /ms/q2/job_one 
hdfs dfs -rm -r /ms/q2/job_two 
hdfs dfs -rm -r /ms/q2/final_answer 

#Q3
hdfs dfs -rm -r /ms/q3/final_answer 

#Q4
hdfs dfs -rm -r /ms/q4/job_one 
hdfs dfs -rm -r /ms/q4/job_two 
hdfs dfs -rm -r /ms/q4/final_answer 

#Q5
hdfs dfs -rm -r /ms/q5/job_one
hdfs dfs -rm -r /ms/q5/final_answer

#Q6
hdfs dfs -rm -r /ms/q6/final_answer

#Q7
hdfs dfs -rm -r /ms/q7/job_one
hdfs dfs -rm -r /ms/q7/job_two
hdfs dfs -rm -r /ms/q7/job_three
hdfs dfs -rm -r /ms/q7/final_answer
hdfs dfs -rm -r /user/jaredk1/ms/


# Q8
hdfs dfs -rm -r /ms/q8/job_one
hdfs dfs -rm -r /ms/q8/job_two
hdfs dfs -rm -r /ms/q8/job_three
hdfs dfs -rm -r /ms/q8/final_answer

# Q9
hdfs dfs -rm -r /ms/q9/job_one
hdfs dfs -rm -r /ms/q9/job_two
hdfs dfs -rm -r /ms/q9/job_three
hdfs dfs -rm -r /ms/q9/terms
hdfs dfs -rm -r /ms/q9/global_top_terms
hdfs dfs -rm -r /ms/q9/final_answer


# Q10
hdfs dfs -rm -r /ms/q10/job_one
hdfs dfs -rm -r /ms/q10/job_two
hdfs dfs -rm -r /ms/q10/job_three
hdfs dfs -rm -r /ms/q10/job_four
hdfs dfs -rm -r /ms/q10/job_five
hdfs dfs -rm -r /ms/q10/IDF
hdfs dfs -rm -r /ms/q10/playlist




# run the map reduce jobs
hadoop jar HW3-Hadoop-1.0-SNAPSHOT.jar csx55.hadoop.MasterDriver


# # # Output the results
hdfs dfs -cat /ms/q1/final_answer/part-r-00000 | head -n 1
hdfs dfs -cat /ms/q2/final_answer/part-r-00000 | head -n 1
hdfs dfs -cat /ms/q3/final_answer/part-r-00000 | head -n 2
hdfs dfs -cat /ms/q4/final_answer/part-r-00000 | head -n 1
hdfs dfs -cat /ms/q5/final_answer/part-r-00000
hdfs dfs -cat /ms/q6/final_answer/part-r-00000
#hdfs dfs -cat /ms/q7/final_answer/part-r-00000 | head -n 1
hdfs dfs -cat /ms/q8/final_answer/part-r-00000

hdfs dfs -cat /ms/q9/final_answer/part-r-00000
hdfs dfs -cat /ms/q9/global_top_terms/part-r-00000 | head -n 10

hdfs dfs -cat /ms/q10/playlist/part-r-00000 | tail -n 10