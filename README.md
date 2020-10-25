# DataEngineerChallenge

This is an interview challenge for PayPay. Please feel free to fork. Pull Requests will be ignored.

The challenge is to make make analytical observations about the data using the distributed tools below.

## Processing & Analytical goals:

1. Sessionize the web log by IP. Sessionize = aggregrate all page hits by visitor/IP during a session.
    https://en.wikipedia.org/wiki/Session_(web_analytics)

> I've defined session with `ip_session_info` which has ip with count of session

Result looks like this
```
2020-10-25 00:57:39.679 | INFO     | __main__:sessionize:41 - 1. Sessionize the web log by IP = aggregate all page hits by visitor/IP during a session
+-----------------+-------------+
|  ip_session_info|count_session|
+-----------------+-------------+
| 220.226.206.7_12|           12|
| 220.226.206.7_11|           11|
| 220.226.206.7_10|           10|
| 176.34.159.236_9|            9|
|   54.228.16.12_9|            9|
| 54.250.253.236_9|            9|
|   185.20.4.220_9|            9|
| 120.29.232.107_9|            9|
|168.235.197.238_9|            9|
|  54.241.32.108_9|            9|
|  119.81.61.166_9|            9|
| 177.71.207.172_9|            9|
|   54.232.40.76_9|            9|
| 54.255.254.236_9|            9|
|  54.243.31.236_9|            9|
|  54.244.52.204_9|            9|
|  107.23.255.12_9|            9|
|  54.245.168.44_9|            9|
|  54.252.79.172_9|            9|
|  54.240.196.33_9|            9|
+-----------------+-------------+
only showing top 20 rows
```
Details are in `processing_analytics_challenge.py`

2. Determine the average session time
> I've defined `current_timestamp - previous_timestamp` value for each session, and get average value of all.

Result looks like:
```
2020-10-25 02:54:18.696 | INFO     | __main__:average_session_time:70 - 2. Determine the average session time

+------------------+
|  avg_session_time|
+------------------+
|125.39079162978608|
+------------------+
``` 

3. Determine unique URL visits per session. To clarify, count a hit to a unique URL only once per session.
> I've split request parameter in log file into 3 parts. request_action, request_url and request_protocol 
> 
> I counted unique request_url associated with ip_address

Result looks like:
```
2020-10-25 01:01:44.678 | INFO     | __main__:count_unique_request:90 - 3. Determine unique URL visits per session. To clarify, count a hit to a unique URL only once per session
+-----------------+---------------------+
|  ip_session_info|count_unique_requests|
+-----------------+---------------------+
| 115.111.50.254_1|                   18|
|115.248.233.203_2|                   86|
| 122.164.34.125_0|                    8|
|205.175.226.101_0|                   89|
|115.242.129.233_0|                    7|
|    1.39.61.253_0|                   59|
|117.239.224.160_1|                   64|
| 223.255.247.66_0|                    7|
|  188.40.94.195_1|                   89|
|  192.193.164.9_1|                   55|
|   202.91.134.7_4|                   10|
| 115.249.21.130_0|                   10|
| 117.210.14.119_0|                    3|
|  106.51.235.51_0|                   89|
|    8.37.228.47_1|                   55|
|  101.57.193.44_0|                   82|
|  182.68.136.65_0|                  104|
|   182.69.48.36_0|                  108|
| 59.165.251.191_2|                   86|
| 59.184.184.157_0|                    9|
+-----------------+---------------------+
20 rows only
``` 
4. Find the most engaged users, ie the IPs with the longest session times
> Like question number 2, I've defined I've defined `current_timestamp - previous_timestamp` value for each session.

Result looks like:
```
2020-10-25 02:57:59.440 | INFO     | __main__:get_longest_session_time:105 - 4. Find the most engaged users, ie the IPs with the longest session times

+--------------+------------------+
|     client_ip|total_session_time|
+--------------+------------------+
|  52.74.219.71| 66621.25013548415|
| 119.81.61.166| 66620.88388029093|
| 106.186.23.95| 66613.19862824267|
| 54.251.151.39| 66611.15383011854|
|117.104.239.89| 66606.24602496746|
|  46.236.24.51| 66603.57164850831|
|   192.0.84.33|  66600.7610411644|
| 178.255.152.2| 66598.93626964092|
|184.75.208.210| 66598.74235332012|
|54.252.254.204| 66595.46702957153|
|  46.236.24.53| 66594.18781757355|
|177.71.207.172| 66592.67154693604|
|  185.20.4.220| 66588.39714431763|
| 54.252.79.172| 66588.26138305664|
|   202.3.66.50|  66582.3921978694|
|117.239.35.226| 66579.01643550312|
|54.183.255.140| 66579.01435089111|
| 107.23.255.12| 66560.83276367188|
|121.58.175.128|  66560.2263838835|
|  207.46.13.22| 66559.47414444387|
+--------------+------------------+
only showing top 20 rows

```
## Additional questions for Machine Learning Engineer (MLE) candidates:
1. Predict the expected load (requests/second) in the next minute

> I used data as time series format and used facebook time series library called fbprophet. There are so many ways to solve this
> Like: Seq2Seq LSTM n/w, popular time series techniques like ARIMA or holt_winter or ensembling approach with some regression models, even spark mllib library can also be used to do it
> As of now for the sake of simplicity and lack of time on my side, I used prophet and it's additive modeling approach is decent to start. 
>
>Approach I used is simply prepared the data as a time series manner like below 

```
+-------------------+----+
|               time|load|
+-------------------+----+
|2015-07-22 11:40:06|  27|
|2015-07-22 11:40:07|  62|
|2015-07-22 11:40:08|  56|
|2015-07-22 11:40:09| 112|
|2015-07-22 11:40:10|  58|
```
> And used fbprophet to predict the future load. **details are in ml_task1.ipynb**

2. Predict the session length for a given IP
> Split the ip_address into 4 parts, octet0, octet1, octet2, octet3 and calculated the session length as shown below
```
+------------------+------+------+------+------+
|    session_length|octet0|octet1|octet2|octet3|
+------------------+------+------+------+------+
| 69.81707191467285|     1|   186|    41|     1|
| 231.7906957184896|     1|   186|    76|    11|
| 33.04862296581268|     1|   187|   228|   210|
| 33.92300724051893|     1|   187|   228|    88|
```
> Then trained most popular gradient boosting alogorithm `xgboost` and `randomforest` to predict the future session length.

**details are in ml_task2.ipynb**

3. Predict the number of unique URL visits by a given IP
> similar as task 2 of ML I followed here divided the ip address into 4 parts called octet
> calculated the unique url count 
> trained `xgboost` and `randomforest` to predict the unique url visit. 
>
```
+-----------------+------+------+------+------+
|count_unique_URLs|octet0|octet1|octet2|octet3|
+-----------------+------+------+------+------+
|               84|   113|   193|   114|    25|
|               85|   115|   112|   250|   108|
|                2|   117|   203|   181|   144|
|                7|   120|    61|    47|    36|
|               88|   124|   125|    22|   218|
|              108|    14|   139|    82|   134|

```
**details are in ml_task3.ipynb including performance measure of models**

## Note: 
    Usually for regression problems I mostly train more than one model and for final prediction
    I ensemble them all with some weights as per the performance. 
    Here due to limited time I followed simple approaches to finish things on time. 

## Tools allowed (in no particular order):
- Spark (any language, but prefer Scala or Java)
- Pig
- MapReduce (Hadoop 2.x only)
- Flink
- Cascading, Cascalog, or Scalding

If you need Hadoop, we suggest 
HDP Sandbox:
http://hortonworks.com/hdp/downloads/
or 
CDH QuickStart VM:
http://www.cloudera.com/content/cloudera/en/downloads.html


### Additional notes:
- You are allowed to use whatever libraries/parsers/solutions you can find provided you can explain the functions you are implementing in detail.
- IP addresses do not guarantee distinct users, but this is the limitation of the data. As a bonus, consider what additional data would help make better analytical conclusions
- For this dataset, complete the sessionization by time window rather than navigation. Feel free to determine the best session window time on your own, or start with 15 minutes.
- The log file was taken from an AWS Elastic Load Balancer:
http://docs.aws.amazon.com/ElasticLoadBalancing/latest/DeveloperGuide/access-log-collection.html#access-log-entry-format



## How to complete this challenge:

1. Fork this repo in github
2. Complete the processing and analytics as defined first to the best of your ability with the time provided.
3. Place notes in your code to help with clarity where appropriate. Make it readable enough to present to the PayPay interview team.
4. Include the test code and data in your solution. 
5. Complete your work in your own github repo and send the results to us and/or present them during your interview.

## What are we looking for? What does this prove?

We want to see how you handle:
- New technologies and frameworks
- Messy (ie real) data
- Understanding data transformation
This is not a pass or fail test, we want to hear about your challenges and your successes with this particular problem.
