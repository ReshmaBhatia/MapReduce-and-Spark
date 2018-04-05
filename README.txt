Python Version : 3.4
Spark Version : 2.2.1


Task 1 : 
bin/hadoop jar Reshma_Bhatia_avg.jar Reshma_Bhatia_Average input-dir output-dir


Task 2:
bin/spark-submit Reshma_Bhatia_Average.py menu.csv output-folder
We save our python code for task 2 on our ec2-server. We need to install pyspark on our ec2-instance to complete task 2 of this assignment.
After these preliminary steps, execute the following command:

spark-2.2.1-bin-hadoop2.7/bin/spark-submit Reshma_Bhatia_Average.py menu.csv output-r

here,
Reshma_Bhatia_Average.py is the python file which has code for task2 in it
menu.csv is the file given to work on in task2
output-r is the output directory in which we wish to have our output file(i.e Reshma_Bhatia_task2.txt)

we can check our output using following commands::

ls output-r   (to open the output directory)
vi Reshma_Bhatia_task2.txt (to read the output file)


 
