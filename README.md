# Airflow_ApiCallspipe

This Pipe allows to get data from a hubspot owned account, specifically get from contacts tables and save into Snowflake DWH.  

Steps are:
 1.using keys get data from hubspot api
 2.Using data get the id and all properties needed.
 3.Move the results to aws s3 datalake
 4.Move to snowflake temp table
 5.Aplly merge to master table using temp table in Snowflake.
 
The Following images shows the execution Dag

![image](https://user-images.githubusercontent.com/5835040/116147472-62109280-a6a5-11eb-82f8-66977f47c11f.png)

Logs :

![image](https://user-images.githubusercontent.com/5835040/116147567-840a1500-a6a5-11eb-9be1-cd86e581da22.png)

TreeView execution log :

![image](https://user-images.githubusercontent.com/5835040/116147677-a0a64d00-a6a5-11eb-8c04-62871f3b6b07.png)
