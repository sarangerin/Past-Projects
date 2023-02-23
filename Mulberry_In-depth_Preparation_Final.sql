#############################################################################################################################################################
## In-depth Analysis from 2011 to 202205 by Erin, 20220816
## Group 1: Traffics with checkout journey
##############################################################################################################################################################
# temp_work_202205
## group1_work_202205
### group1_session_202205
#### group1_session_checkout_202205
##### group1_session_checkout_pg_202205
###### group1_session_checkout_pg_merged_202205
####### group1_final_202205
###############################################################
set sql_safe_updates = 0;
# Let Group 1: Traffics with checout journey ##

#1. Create temp_work: only extract traffics from the ips that have checkout journey

### Extraction from the Work Table
#### Where remote_addr equals IPs that have http_referrer like "%mlb%" in at least one traffic

Create Table group1_ip_list_202205 As
Select distinct(remote_addr) from work_KR_web_logs_202205
Where http_referrer like "%mlb%" or http_referrer like "%account%"
;


Drop Table If Exists temp_work_202205;
Create Table temp_work_202205 As
Select * From work_KR_web_logs_202205
Where work_KR_web_logs_202205.remote_addr IN (Select remote_addr from group1_ip_list_202205)
;

#### Delete observations where remote_addr is blank
Delete from temp_work_202205
Where http_referrer = " "
;


# 1(1) Create hhmmss as timestamp variable (data type conversion) in the temp_work tables


UPDATE temp_work_202205
set hhmmss = STR_TO_DATE(substring_index(timestamp, 'Z', 1), '%Y-%m-%dT%H:%i:%s.%f');

ALTER TABLE temp_work_202205
ADD COLUMN ip_device_id varchar(500)
;

UPDATE temp_work_202205
SET device = "NA"
Where device is null
;

UPDATE temp_work_202205
SET ip_device_id = concat(remote_addr, " ", device)
;
Set sql_safe_updates = 0;

#2. Create group1_work monthly tables with lag variables (i.e., lag_hhmmss and lag_duration)

Drop Table if exists group1_work_202205
;
Create Table group1_work_202205 As
SELECT device, remote_addr, ip_device_id, http_referrer, hhmmss,
LAG(hhmmss) OVER (Partition by ip_device_id Order by hhmmss) As lag_hhmmss
FROM temp_work_202205
;

Alter Table group1_work_202205 
Add Column lag_duration Int
;


Update group1_work_202205
Set lag_duration = ABS(timestampdiff(microsecond, hhmmss, lag_hhmmss));

Show warnings;


# 3. Identify sessions for each group1_work table at Python, save it back to the DB as group1_session_YYYYMM ###

# 4. Create new monthly tables group1_session_checkout_yyyymm by removing sessions without checout journey

Drop Table if Exists session_list;
Create table session_list
As
Select distinct(session) as session_id
From group1_session_202205
Where Replace(http_referrer, "%", "") like "%/mlb/%" or http_referrer like "%account%"
;

Create table group1_session_checkout_202205
As
Select * From group1_session_202205
Where session IN (Select session_id from session_list)
;

# 5. Work on python to page_id, save as group1_session_checkout_pg_202205

#6. Create group1_session_checkout_pg_merged_202205 Within session, merge rows that are consecutive and and have identiccal http_referrer

Create Table group1_session_checkout_pg_merged_202205
Select device, remote_addr, ip_device_id, http_referrer,
	MIN(hhmmss) as hhmmss, session, page
From group1_session_checkout_pg_202205
Group by device, remote_addr, session, page
;

# 6(1). Make lead variables used to calculate per page duration

Alter Table group1_session_checkout_pg_merged_202205
Modify Column hhmmss varchar(500),
Modify Column session int,
Modify Column page int
;

# 7. Create a final monthly table for in-depth analysis

Create Table group1_final_202205
As
Select device, remote_addr, http_referrer, session, page, hhmmss,
	Lead(hhmmss) over (partition by session order by hhmmss) as lead_hhmmss
From group1_session_checkout_pg_merged_202205
;

#7(1). unit conversion for per-page duration from microsecond to second, a new variable named page_duration
Alter Table group1_final_202205
Add Column page_duration Decimal(30,3)
;

Update group1_final_202205
Set page_duration = ABS(timestampdiff(microsecond, hhmmss, lead_hhmmss))/1000000
;

# 7(2). Change the timezone from UK to Seoul, Korea
Update group1_final_202205
Set hhmmss = CONVERT_TZ(hhmmss, '+00:00', '+09:00'), lead_hhmmss = convert_tz(lead_hhmmss, '+00:00', '+09:00')
;

### Repeat (1) through to (6) starting with temp_work_YYYYMM files as the final data table for in-depth analysis

#### 202111
#### (1) Create hhmmss, lag_hhmmss, lead_hhmmss, lag_duration and lead_duration.


UPDATE temp_work_202111
set hhmmss = STR_TO_DATE(substring_index(timestamp, 'Z', 1), '%Y-%m-%dT%H:%i:%s.%f');

ALTER TABLE temp_work_202111
ADD COLUMN ip_device_id varchar(500)
;

UPDATE temp_work_202111
SET device = "NA"
Where device is null
;

UPDATE temp_work_202111
SET ip_device_id = concat(remote_addr, " ", device)
;
Set sql_safe_updates = 0;

Drop Table if exists group1_work_202111
;
Create Table group1_work_202111 As
SELECT device, remote_addr, ip_device_id, http_referrer, hhmmss,
    LAG(hhmmss) OVER (Partition by ip_device_id Order by hhmmss) As lag_hhmmss
FROM temp_work_202111
;

Alter Table group1_work_202111 
Add Column lag_duration Int
;


Update group1_work_202111
Set lag_duration = ABS(timestampdiff(microsecond, hhmmss, lag_hhmmss));

Show warnings;


### (2)  Identify sessions for each group1_work table at Python, save it back to the DB as group1_session_YYYYMM ###

### (3) Delete sessions without checkout journey ###

Drop Table if Exists session_list;
Create table session_list
As
Select distinct(session) as session_id
From group1_session_202111
Where Replace(http_referrer, "%", "") like "%/mlb/%" or http_referrer like "%account%"
;

Create table group1_session_checkout_202111
As
Select * From group1_session_202111
Where session IN (Select session_id from session_list)
;

### (4) Within session, merge rows that are consecutive and and have identiccal http_referrer
#### (4)-1 Work on python to page_id, save as group1_session_checkout_pg_202111
#### (4)-2 Create group1_session_checkout_pg_merged_202111

Create Table group1_session_checkout_pg_merged_202111
Select device, remote_addr, ip_device_id, http_referrer,
	MIN(hhmmss) as hhmmss, session, page
From group1_session_checkout_pg_202111
Group by device, remote_addr, session, page
;

### (5) Make lead variable and page duration (microsecond than change to second with three digits of decimals)

Alter Table group1_session_checkout_pg_merged_202111
Modify Column hhmmss varchar(500),
Modify Column session int,
Modify Column page int
;

Create Table group1_final_202111
As
Select device, remote_addr, http_referrer, session, page, hhmmss,
	Lead(hhmmss) over (partition by session order by hhmmss) as lead_hhmmss
From group1_session_checkout_pg_merged_202111
;

Alter Table group1_final_202111
Add Column page_duration Decimal(30,3)
;

Update group1_final_202111
Set page_duration = ABS(timestampdiff(microsecond, hhmmss, lead_hhmmss))/1000000
;

### (6) Change the timezone from UK to Seoul, Korea
Update group1_final_202111
Set hhmmss = CONVERT_TZ(hhmmss, '+00:00', '+09:00'), lead_hhmmss = convert_tz(lead_hhmmss, '+00:00', '+09:00')
;

#### 202112
#### (1) Create hhmmss, lag_hhmmss, lead_hhmmss, lag_duration and lead_duration.


UPDATE temp_work_202112
set hhmmss = STR_TO_DATE(substring_index(timestamp, 'Z', 1), '%Y-%m-%dT%H:%i:%s.%f');

ALTER TABLE temp_work_202112
ADD COLUMN ip_device_id varchar(500)
;

UPDATE temp_work_202112
SET device = "NA"
Where device is null
;

UPDATE temp_work_202112
SET ip_device_id = concat(remote_addr, " ", device)
;
Set sql_safe_updates = 0;

Drop Table if exists group1_work_202112
;
Create Table group1_work_202112 As
SELECT device, remote_addr, ip_device_id, http_referrer, hhmmss,
    LAG(hhmmss) OVER (Partition by ip_device_id Order by hhmmss) As lag_hhmmss
FROM temp_work_202112
;

Alter Table group1_work_202112 
Add Column lag_duration Int
;


Update group1_work_202112
Set lag_duration = ABS(timestampdiff(microsecond, hhmmss, lag_hhmmss));

Show warnings;


### (2)  Identify sessions for each group1_work table at Python, save it back to the DB as group1_session_YYYYMM ###

### (3) Delete sessions without checkout journey ###

Drop Table if Exists session_list;
Create table session_list
As
Select distinct(session) as session_id
From group1_session_202112
Where Replace(http_referrer, "%", "") like "%/mlb/%" or http_referrer like "%account%"
;

Create table group1_session_checkout_202112
As
Select * From group1_session_202112
Where session IN (Select session_id from session_list)
;

### (4) Within session, merge rows that are consecutive and and have identiccal http_referrer
#### (4)-1 Work on python to page_id, save as group1_session_checkout_pg_202112
#### (4)-2 Create group1_session_checkout_pg_merged_202112

Create Table group1_session_checkout_pg_merged_202112
Select device, remote_addr, ip_device_id, http_referrer,
	MIN(hhmmss) as hhmmss, session, page
From group1_session_checkout_pg_202112
Group by device, remote_addr, session, page
;

### (5) Make lead variable and page duration (microsecond than change to second with three digits of decimals)

Alter Table group1_session_checkout_pg_merged_202112
Modify Column hhmmss varchar(500),
Modify Column session int,
Modify Column page int
;

Create Table group1_final_202112
As
Select device, remote_addr, http_referrer, session, page, hhmmss,
	Lead(hhmmss) over (partition by session order by hhmmss) as lead_hhmmss
From group1_session_checkout_pg_merged_202112
;

Alter Table group1_final_202112
Add Column page_duration Decimal(30,3)
;

Update group1_final_202112
Set page_duration = ABS(timestampdiff(microsecond, hhmmss, lead_hhmmss))/1000000
;

### (6) Change the timezone from UK to Seoul, Korea
Update group1_final_202112
Set hhmmss = CONVERT_TZ(hhmmss, '+00:00', '+09:00'), lead_hhmmss = convert_tz(lead_hhmmss, '+00:00', '+09:00')
;

#### 202201
#### (1) Create hhmmss, lag_hhmmss, lead_hhmmss, lag_duration and lead_duration.


UPDATE temp_work_202201
set hhmmss = STR_TO_DATE(substring_index(timestamp, 'Z', 1), '%Y-%m-%dT%H:%i:%s.%f');

ALTER TABLE temp_work_202201
ADD COLUMN ip_device_id varchar(500)
;

UPDATE temp_work_202201
SET device = "NA"
Where device is null
;

UPDATE temp_work_202201
SET ip_device_id = concat(remote_addr, " ", device)
;
Set sql_safe_updates = 0;

Drop Table if exists group1_work_202201
;
Create Table group1_work_202201 As
SELECT device, remote_addr, ip_device_id, http_referrer, hhmmss,
    LAG(hhmmss) OVER (Partition by ip_device_id Order by hhmmss) As lag_hhmmss
FROM temp_work_202201
;

Alter Table group1_work_202201 
Add Column lag_duration Int
;


Update group1_work_202201
Set lag_duration = ABS(timestampdiff(microsecond, hhmmss, lag_hhmmss));

Show warnings;


### (2)  Identify sessions for each group1_work table at Python, save it back to the DB as group1_session_YYYYMM ###

### (3) Delete sessions without checkout journey ###

Drop Table if Exists session_list;
Create table session_list
As
Select distinct(session) as session_id
From group1_session_202201
Where Replace(http_referrer, "%", "") like "%/mlb/%" or http_referrer like "%account%"
;

Create table group1_session_checkout_202201
As
Select * From group1_session_202201
Where session IN (Select session_id from session_list)
;

### (4) Within session, merge rows that are consecutive and and have identiccal http_referrer
#### (4)-1 Work on python to page_id, save as group1_session_checkout_pg_202201
#### (4)-2 Create group1_session_checkout_pg_merged_202201

Create Table group1_session_checkout_pg_merged_202201
Select device, remote_addr, ip_device_id, http_referrer,
	MIN(hhmmss) as hhmmss, session, page
From group1_session_checkout_pg_202201
Group by device, remote_addr, session, page
;

### (5) Make lead variable and page duration (microsecond than change to second with three digits of decimals)

Alter Table group1_session_checkout_pg_merged_202201
Modify Column hhmmss varchar(500),
Modify Column session int,
Modify Column page int
;

Create Table group1_final_202201
As
Select device, remote_addr, http_referrer, session, page, hhmmss,
	Lead(hhmmss) over (partition by session order by hhmmss) as lead_hhmmss
From group1_session_checkout_pg_merged_202201
;

Alter Table group1_final_202201
Add Column page_duration Decimal(30,3)
;

Update group1_final_202201
Set page_duration = ABS(timestampdiff(microsecond, hhmmss, lead_hhmmss))/1000000
;

### (6) Change the timezone from UK to Seoul, Korea
Update group1_final_202201
Set hhmmss = CONVERT_TZ(hhmmss, '+00:00', '+09:00'), lead_hhmmss = convert_tz(lead_hhmmss, '+00:00', '+09:00')
;

#### 202202
#### (1) Create hhmmss, lag_hhmmss, lead_hhmmss, lag_duration and lead_duration.


UPDATE temp_work_202202
set hhmmss = STR_TO_DATE(substring_index(timestamp, 'Z', 1), '%Y-%m-%dT%H:%i:%s.%f');

ALTER TABLE temp_work_202202
ADD COLUMN ip_device_id varchar(500)
;

UPDATE temp_work_202202
SET device = "NA"
Where device is null
;

UPDATE temp_work_202202
SET ip_device_id = concat(remote_addr, " ", device)
;
Set sql_safe_updates = 0;

Drop Table if exists group1_work_202202
;
Create Table group1_work_202202 As
SELECT device, remote_addr, ip_device_id, http_referrer, hhmmss,
    LAG(hhmmss) OVER (Partition by ip_device_id Order by hhmmss) As lag_hhmmss
FROM temp_work_202202
;

Alter Table group1_work_202202 
Add Column lag_duration Int
;


Update group1_work_202202
Set lag_duration = ABS(timestampdiff(microsecond, hhmmss, lag_hhmmss));

Show warnings;


### (2)  Identify sessions for each group1_work table at Python, save it back to the DB as group1_session_YYYYMM ###

### (3) Delete sessions without checkout journey ###

Drop Table if Exists session_list;
Create table session_list
As
Select distinct(session) as session_id
From group1_session_202202
Where Replace(http_referrer, "%", "") like "%/mlb/%" or http_referrer like "%account%"
;

Create table group1_session_checkout_202202
As
Select * From group1_session_202202
Where session IN (Select session_id from session_list)
;

### (4) Within session, merge rows that are consecutive and and have identiccal http_referrer
#### (4)-1 Work on python to page_id, save as group1_session_checkout_pg_202202
#### (4)-2 Create group1_session_checkout_pg_merged_202202

Create Table group1_session_checkout_pg_merged_202202
Select device, remote_addr, ip_device_id, http_referrer,
	MIN(hhmmss) as hhmmss, session, page
From group1_session_checkout_pg_202202
Group by device, remote_addr, session, page
;

### (5) Make lead variable and page duration (microsecond than change to second with three digits of decimals)

Alter Table group1_session_checkout_pg_merged_202202
Modify Column hhmmss varchar(500),
Modify Column session int,
Modify Column page int
;

Create Table group1_final_202202
As
Select device, remote_addr, http_referrer, session, page, hhmmss,
	Lead(hhmmss) over (partition by session order by hhmmss) as lead_hhmmss
From group1_session_checkout_pg_merged_202202
;

Alter Table group1_final_202202
Add Column page_duration Decimal(30,3)
;

Update group1_final_202202
Set page_duration = ABS(timestampdiff(microsecond, hhmmss, lead_hhmmss))/1000000
;

### (6) Change the timezone from UK to Seoul, Korea
Update group1_final_202202
Set hhmmss = CONVERT_TZ(hhmmss, '+00:00', '+09:00'), lead_hhmmss = convert_tz(lead_hhmmss, '+00:00', '+09:00')
;

#### 202203
#### (1) Create hhmmss, lag_hhmmss, lead_hhmmss, lag_duration and lead_duration.


UPDATE temp_work_202203
set hhmmss = STR_TO_DATE(substring_index(timestamp, 'Z', 1), '%Y-%m-%dT%H:%i:%s.%f');

ALTER TABLE temp_work_202203group1_session_checkout_202205group1_session_202205group1_session_202204
ADD COLUMN ip_device_id varchar(500)
;

UPDATE temp_work_202203
SET device = "NA"
Where device is null
;

UPDATE temp_work_202203
SET ip_device_id = concat(remote_addr, " ", device)
;
Set sql_safe_updates = 0;

Drop Table if exists group1_work_202203
;
Create Table group1_work_202203 As
SELECT device, remote_addr, ip_device_id, http_referrer, hhmmss,
    LAG(hhmmss) OVER (Partition by ip_device_id Order by hhmmss) As lag_hhmmss
FROM temp_work_202203
;

Alter Table group1_work_202203 
Add Column lag_duration Int
;


Update group1_work_202203
Set lag_duration = ABS(timestampdiff(microsecond, hhmmss, lag_hhmmss));

Show warnings;


### (2)  Identify sessions for each group1_work table at Python, save it back to the DB as group1_session_YYYYMM ###

### (3) Delete sessions without checkout journey ###

Drop Table if Exists session_list;
Create table session_list
As
Select distinct(session) as session_id
From group1_session_202203
Where Replace(http_referrer, "%", "") like "%/mlb/%" or http_referrer like "%account%"
;

Create table group1_session_checkout_202203
As
Select * From group1_session_202203
Where session IN (Select session_id from session_list)
;

### (4) Within session, merge rows that are consecutive and and have identiccal http_referrer
#### (4)-1 Work on python to page_id, save as group1_session_checkout_pg_202203
#### (4)-2 Create group1_session_checkout_pg_merged_202203

Create Table group1_session_checkout_pg_merged_202203
Select device, remote_addr, ip_device_id, http_referrer,
	MIN(hhmmss) as hhmmss, session, page
From group1_session_checkout_pg_202203
Group by device, remote_addr, session, page
;

### (5) Make lead variable and page duration (microsecond than change to second with three digits of decimals)

Alter Table group1_session_checkout_pg_merged_202203
Modify Column hhmmss varchar(500),
Modify Column session int,
Modify Column page int
;

Create Table group1_final_202203
As
Select device, remote_addr, http_referrer, session, page, hhmmss,
	Lead(hhmmss) over (partition by session order by hhmmss) as lead_hhmmss
From group1_session_checkout_pg_merged_202203
;

Alter Table group1_final_202203
Add Column page_duration Decimal(30,3)
;

Update group1_final_202203
Set page_duration = ABS(timestampdiff(microsecond, hhmmss, lead_hhmmss))/1000000
;

### (6) Change the timezone from UK to Seoul, Korea
Update group1_final_202203
Set hhmmss = CONVERT_TZ(hhmmss, '+00:00', '+09:00'), lead_hhmmss = convert_tz(lead_hhmmss, '+00:00', '+09:00')
;

#### 202204
#### (1) Create hhmmss, lag_hhmmss, lead_hhmmss, lag_duration and lead_duration.


UPDATE temp_work_202204
set hhmmss = STR_TO_DATE(substring_index(timestamp, 'Z', 1), '%Y-%m-%dT%H:%i:%s.%f');

ALTER TABLE temp_work_202204
ADD COLUMN ip_device_id varchar(500)
;

UPDATE temp_work_202204
SET device = "NA"
Where device is null
;

UPDATE temp_work_202204
SET ip_device_id = concat(remote_addr, " ", device)
;
Set sql_safe_updates = 0;

Drop Table if exists group1_work_202204
;
Create Table group1_work_202204 As
SELECT device, remote_addr, ip_device_id, http_referrer, hhmmss,
    LAG(hhmmss) OVER (Partition by ip_device_id Order by hhmmss) As lag_hhmmss
FROM temp_work_202204
;

Alter Table group1_work_202204 
Add Column lag_duration Int
;


Update group1_work_202204
Set lag_duration = ABS(timestampdiff(microsecond, hhmmss, lag_hhmmss));

Show warnings;


### (2)  Identify sessions for each group1_work table at Python, save it back to the DB as group1_session_YYYYMM ###

### (3) Delete sessions without checkout journey ###

Drop Table if Exists session_list;
Create table session_list
As
Select distinct(session) as session_id
From group1_session_202204
Where Replace(http_referrer, "%", "") like "%/mlb/%" or http_referrer like "%account%"
;

Create table group1_session_checkout_202204
As
Select * From group1_session_202204
Where session IN (Select session_id from session_list)
;
###################################STOPPED HERE
### (4) Within session, merge rows that are consecutive and and have identiccal http_referrer
#### (4)-1 Work on python to page_id, save as group1_session_checkout_pg_202204
#### (4)-2 Create group1_session_checkout_pg_merged_202204

Create Table group1_session_checkout_pg_merged_202204
Select device, remote_addr, ip_device_id, http_referrer,
	MIN(hhmmss) as hhmmss, session, page
From group1_session_checkout_pg_202204
Group by device, remote_addr, session, page
;

### (5) Make lead variable and page duration (microsecond than change to second with three digits of decimals)

Alter Table group1_session_checkout_pg_merged_202204
Modify Column hhmmss varchar(500),
Modify Column session int,
Modify Column page int
;

Create Table group1_final_202204
As
Select device, remote_addr, http_referrer, session, page, hhmmss,
	Lead(hhmmss) over (partition by session order by hhmmss) as lead_hhmmss
From group1_session_checkout_pg_merged_202204
;

Alter Table group1_final_202204
Add Column page_duration Decimal(30,3)
;

Update group1_final_202204
Set page_duration = ABS(timestampdiff(microsecond, hhmmss, lead_hhmmss))/1000000
;

### (6) Change the timezone from UK to Seoul, Korea
Update group1_final_202204
Set hhmmss = CONVERT_TZ(hhmmss, '+00:00', '+09:00'), lead_hhmmss = convert_tz(lead_hhmmss, '+00:00', '+09:00')
;


######################################################################################################################################################################
##     From here, all analysis and interpretations be done in session-level:
##
##     1. For Session Summary Index section:
##            (1) Identify the Start Page, Exit Page, Start Timestamp and Exit Time using the maximum and minimum values of 'hhmmss' variable, group by session
##            (2) Calculate the Journey Duration as the time difference between the minimum and maximum values of 'hhmmss' variable
##            (3) Calculate # Pages Visited using count(distinct http_referrer) group by session, and Avg per Page Duration as Journey Duration/ # Pages Visited

##     2. For Checkout Journey:
##            (1) Create a temporary table that includes checkout journey traffics only (with where condition)
##            (2) Count Log-In, Sign-Up, Cart, Delivery and Payment
##            (3) Calculate total duration for Log-In, Sign-Up, Cart, Delivery and Payment separately
##            (4) Identify Last Visited Page by select http_referrer where hhmmss is maximum by session
##            (5) Identify sessions where the checkout was successful (i.e., http_referrer with "%thankyou?%"
##     3. For Product Journey:
##            (1) Create a temporary table that includes only product journey traffics (with where condition)
##            (2) Similar to what's done in 2, identify the http_referrer with highest and lowest durations (i.e., Page with Highest Duration and Page with Lowest Duration) and their respective durations
##            (3) Calculate Total Duration using the maximum and minimum values of 'hhmmss' variable, group by session
##  
################################################################################################################################################################################
### 1. For Session Summary Index section:
###      (1) Identify the Start Page, Exit Page, Start Timestamp and Exit Timestamp using the maximum and minimum values of 'hhmmss' variable, group by session
###      (2) Calculate the Journey Duration as the sum of page_duration
###      (3) Calculate # Pages Visited using count(distinct http_referrer) group by session, and Avg per Page Duration as Journey Duration/ # Pages Visited

Create Table session_summary_index As

WITH TMP AS(
Select *, 
ROW_NUMBER() Over (Partition by Session Order by page asc) as asc_num,
ROW_NUMBER() Over (Partition by Session Order by page desc) as desc_num
From group1_final_202111)

SELECT device, remote_addr, session,
MAX(CASE WHEN asc_num = 1 THEN http_referrer END) start_page,
MAX(CASE WHEN desc_num = 1 THEN http_referrer END) exit_page,
MAX(CASE WHEN asc_num = 1 THEN hhmmss END) start_time,
MAX(CASE WHEN desc_num = 1 THEN hhmmss END) exit_time,
COUNT(*) npages,
SUM(page_duration) total_duration,
SUM(page_duration)/COUNT(*) avg_duration
FROM TMP
GROUP BY device, remote_addr, session
ORDER BY session
;

Insert Into session_summary_index

WITH TMP AS(
Select *, 
ROW_NUMBER() Over (Partition by Session Order by page asc) as asc_num,
ROW_NUMBER() Over (Partition by Session Order by page desc) as desc_num
From group1_final_202112)

SELECT device, remote_addr, session,
MAX(CASE WHEN asc_num = 1 THEN http_referrer END) start_page,
MAX(CASE WHEN desc_num = 1 THEN http_referrer END) exit_page,
MAX(CASE WHEN asc_num = 1 THEN hhmmss END) start_time,
MAX(CASE WHEN desc_num = 1 THEN hhmmss END) exit_time,
COUNT(*) npages,
SUM(page_duration) total_duration,
SUM(page_duration)/COUNT(*) avg_duration
FROM TMP
GROUP BY device, remote_addr, session
ORDER BY session
;

Insert Into session_summary_index

WITH TMP AS(
Select *, 
ROW_NUMBER() Over (Partition by Session Order by page asc) as asc_num,
ROW_NUMBER() Over (Partition by Session Order by page desc) as desc_num
From group1_final_202201)

SELECT device, remote_addr, session,
MAX(CASE WHEN asc_num = 1 THEN http_referrer END) start_page,
MAX(CASE WHEN desc_num = 1 THEN http_referrer END) exit_page,
MAX(CASE WHEN asc_num = 1 THEN hhmmss END) start_time,
MAX(CASE WHEN desc_num = 1 THEN hhmmss END) exit_time,
COUNT(*) npages,
SUM(page_duration) total_duration,
SUM(page_duration)/COUNT(*) avg_duration
FROM TMP
GROUP BY device, remote_addr, session
ORDER BY session
;

Insert Into session_summary_index

WITH TMP AS(
Select *, 
ROW_NUMBER() Over (Partition by Session Order by page asc) as asc_num,
ROW_NUMBER() Over (Partition by Session Order by page desc) as desc_num
From group1_final_202202)

SELECT device, remote_addr, session,
MAX(CASE WHEN asc_num = 1 THEN http_referrer END) start_page,
MAX(CASE WHEN desc_num = 1 THEN http_referrer END) exit_page,
MAX(CASE WHEN asc_num = 1 THEN hhmmss END) start_time,
MAX(CASE WHEN desc_num = 1 THEN hhmmss END) exit_time,
COUNT(*) npages,
SUM(page_duration) total_duration,
SUM(page_duration)/COUNT(*) avg_duration
FROM TMP
GROUP BY device, remote_addr, session
ORDER BY session
;

Insert Into session_summary_index

WITH TMP AS(
Select *, 
ROW_NUMBER() Over (Partition by Session Order by page asc) as asc_num,
ROW_NUMBER() Over (Partition by Session Order by page desc) as desc_num
From group1_final_202203)

SELECT device, remote_addr, session,
MAX(CASE WHEN asc_num = 1 THEN http_referrer END) start_page,
MAX(CASE WHEN desc_num = 1 THEN http_referrer END) exit_page,
MAX(CASE WHEN asc_num = 1 THEN hhmmss END) start_time,
MAX(CASE WHEN desc_num = 1 THEN hhmmss END) exit_time,
COUNT(*) npages,
SUM(page_duration) total_duration,
SUM(page_duration)/COUNT(*) avg_duration
FROM TMP
GROUP BY device, remote_addr, session
ORDER BY session
;

Insert Into session_summary_index

WITH TMP AS(
Select *, 
ROW_NUMBER() Over (Partition by Session Order by page asc) as asc_num,
ROW_NUMBER() Over (Partition by Session Order by page desc) as desc_num
From group1_final_202204)

SELECT device, remote_addr, session,
MAX(CASE WHEN asc_num = 1 THEN http_referrer END) start_page,
MAX(CASE WHEN desc_num = 1 THEN http_referrer END) exit_page,
MAX(CASE WHEN asc_num = 1 THEN hhmmss END) start_time,
MAX(CASE WHEN desc_num = 1 THEN hhmmss END) exit_time,
COUNT(*) npages,
SUM(page_duration) total_duration,
SUM(page_duration)/COUNT(*) avg_duration
FROM TMP
GROUP BY device, remote_addr, session
ORDER BY session
;

Insert Into session_summary_index

WITH TMP AS(
Select *, 
ROW_NUMBER() Over (Partition by Session Order by page asc) as asc_num,
ROW_NUMBER() Over (Partition by Session Order by page desc) as desc_num
From group1_final_202205)

SELECT device, remote_addr, session,
MAX(CASE WHEN asc_num = 1 THEN http_referrer END) start_page,
MAX(CASE WHEN desc_num = 1 THEN http_referrer END) exit_page,
MAX(CASE WHEN asc_num = 1 THEN hhmmss END) start_time,
MAX(CASE WHEN desc_num = 1 THEN hhmmss END) exit_time,
COUNT(*) npages,
SUM(page_duration) total_duration,
SUM(page_duration)/COUNT(*) avg_duration
FROM TMP
GROUP BY device, remote_addr, session
ORDER BY session
;

Create Table session_summary_index_total
As
Select
row_number() over (order by start_time) as session_id,
remote_addr, device, start_page, exit_page, start_time, exit_time, total_duration, npages, avg_duration
From session_summary_index
;

######End of Session Summary Index######


###########################################################################################################################################
##     2. For Checkout Journey:
##            (1) Create a temporary table that includes checkout journey traffics only (with where condition)
##            (2) Count Log-In, Sign-Up, Cart, Delivery and Payment
##            (3) Calculate total duration for Log-In, Sign-Up, Cart, Delivery and Payment separately
##            (4) Identify Last Visited Page by select http_referrer where hhmmss is maximum by session
##            (5) Identify sessions where the checkout was successful (i.e., http_referrer with "%thankyou?%"

Create Table checkout_journey

WITH TMP as (
Select *,
Row_number() over (Partition by session Order by page asc) as asc_num,
Row_number() over (Partition by session Order by page desc) as desc_num

From group1_final_202111 Where Replace(http_referrer, '%', '') like "%/mlb%" or http_referrer like "%account%")

Select device, remote_addr, session, start_time,

count(Case When Replace(http_referrer, '%', '')like "%login%" Then asc_num End) login_attempt,
sum(Case When Replace(http_referrer, '%', '') like "%login%" Then page_duration End) login_duration,
count(Case When Replace(http_referrer, '%', '') like "%create_account%" Then asc_num End) signup_attempt,
sum(Case When Replace(http_referrer, '%', '') like "%create_account%" Then page_duration End) signup_duration,
count(Case When Replace(http_referrer, '%', '') like "%shopping-bag%" Then asc_num End) cart_attempt,
sum(Case When Replace(http_referrer, '%', '') like "%shopping-bag%" Then page_duration End) cart_duration,
count(Case When Replace(http_referrer, '%', '') like "%delivery%" Then asc_num End) delivery_attempt,
sum(Case When Replace(http_referrer, '%', '') like "%delivery%" Then page_duration End) delivery_duration,
count(Case When Replace(http_referrer, '%', '') like "%payment%" Then asc_num End) payment_attempt,
sum(Case When Replace(http_referrer, '%', '') like "%payment%" Then page_duration End) payment_duration,
count(Case When Replace(http_referrer, '%', '') like "%thankyou%" Then asc_num End) checkout_status,
max(Case When desc_num =1 Then http_referrer End) checkout_exit_page,
sum(page_duration) as total_checkout_duration

From TMP
Group by device, remote_addr, session
Order by start_time
;

Insert Into checkout_journey

WITH TMP as (
Select *,
Row_number() over (Partition by session Order by page asc) as asc_num,
Row_number() over (Partition by session Order by page desc) as desc_num

From group1_final_202112 Where Replace(http_referrer, '%', '') like "%/mlb%" or http_referrer like "%account%")

Select device, remote_addr, session, start_time,

count(Case When Replace(http_referrer, '%', '')like "%login%" Then asc_num End) login_attempt,
sum(Case When Replace(http_referrer, '%', '') like "%login%" Then page_duration End) login_duration,
count(Case When Replace(http_referrer, '%', '') like "%create_account%" Then asc_num End) signup_attempt,
sum(Case When Replace(http_referrer, '%', '') like "%create_account%" Then page_duration End) signup_duration,
count(Case When Replace(http_referrer, '%', '') like "%shopping-bag%" Then asc_num End) cart_attempt,
sum(Case When Replace(http_referrer, '%', '') like "%shopping-bag%" Then page_duration End) cart_duration,
count(Case When Replace(http_referrer, '%', '') like "%delivery%" Then asc_num End) delivery_attempt,
sum(Case When Replace(http_referrer, '%', '') like "%delivery%" Then page_duration End) delivery_duration,
count(Case When Replace(http_referrer, '%', '') like "%payment%" Then asc_num End) payment_attempt,
sum(Case When Replace(http_referrer, '%', '') like "%payment%" Then page_duration End) payment_duration,
count(Case When Replace(http_referrer, '%', '') like "%thankyou%" Then asc_num End) checkout_status,
max(Case When desc_num =1 Then http_referrer End) checkout_exit_page,
sum(page_duration) as total_checkout_duration

From TMP
Group by device, remote_addr, session
Order by start_time
;

Insert Into checkout_journey

WITH TMP as (
Select *,
Row_number() over (Partition by session Order by page asc) as asc_num,
Row_number() over (Partition by session Order by page desc) as desc_num

From group1_final_202201 Where Replace(http_referrer, '%', '') like "%/mlb%" or http_referrer like "%account%")

Select device, remote_addr, session, start_time,

count(Case When Replace(http_referrer, '%', '')like "%login%" Then asc_num End) login_attempt,
sum(Case When Replace(http_referrer, '%', '') like "%login%" Then page_duration End) login_duration,
count(Case When Replace(http_referrer, '%', '') like "%create_account%" Then asc_num End) signup_attempt,
sum(Case When Replace(http_referrer, '%', '') like "%create_account%" Then page_duration End) signup_duration,
count(Case When Replace(http_referrer, '%', '') like "%shopping-bag%" Then asc_num End) cart_attempt,
sum(Case When Replace(http_referrer, '%', '') like "%shopping-bag%" Then page_duration End) cart_duration,
count(Case When Replace(http_referrer, '%', '') like "%delivery%" Then asc_num End) delivery_attempt,
sum(Case When Replace(http_referrer, '%', '') like "%delivery%" Then page_duration End) delivery_duration,
count(Case When Replace(http_referrer, '%', '') like "%payment%" Then asc_num End) payment_attempt,
sum(Case When Replace(http_referrer, '%', '') like "%payment%" Then page_duration End) payment_duration,
count(Case When Replace(http_referrer, '%', '') like "%thankyou%" Then asc_num End) checkout_status,
max(Case When desc_num =1 Then http_referrer End) checkout_exit_page,
sum(page_duration) as total_checkout_duration

From TMP
Group by device, remote_addr, session
Order by start_time
;

Insert Into checkout_journey

WITH TMP as (
Select *,
Row_number() over (Partition by session Order by page asc) as asc_num,
Row_number() over (Partition by session Order by page desc) as desc_num

From group1_final_202202 Where Replace(http_referrer, '%', '') like "%/mlb%" or http_referrer like "%account%")

Select device, remote_addr, session, start_time,

count(Case When Replace(http_referrer, '%', '')like "%login%" Then asc_num End) login_attempt,
sum(Case When Replace(http_referrer, '%', '') like "%login%" Then page_duration End) login_duration,
count(Case When Replace(http_referrer, '%', '') like "%create_account%" Then asc_num End) signup_attempt,
sum(Case When Replace(http_referrer, '%', '') like "%create_account%" Then page_duration End) signup_duration,
count(Case When Replace(http_referrer, '%', '') like "%shopping-bag%" Then asc_num End) cart_attempt,
sum(Case When Replace(http_referrer, '%', '') like "%shopping-bag%" Then page_duration End) cart_duration,
count(Case When Replace(http_referrer, '%', '') like "%delivery%" Then asc_num End) delivery_attempt,
sum(Case When Replace(http_referrer, '%', '') like "%delivery%" Then page_duration End) delivery_duration,
count(Case When Replace(http_referrer, '%', '') like "%payment%" Then asc_num End) payment_attempt,
sum(Case When Replace(http_referrer, '%', '') like "%payment%" Then page_duration End) payment_duration,
count(Case When Replace(http_referrer, '%', '') like "%thankyou%" Then asc_num End) checkout_status,
max(Case When desc_num =1 Then http_referrer End) checkout_exit_page,
sum(page_duration) as total_checkout_duration

From TMP
Group by device, remote_addr, session
Order by start_time
;

Insert Into checkout_journey

WITH TMP as (
Select *,
Row_number() over (Partition by session Order by page asc) as asc_num,
Row_number() over (Partition by session Order by page desc) as desc_num

From group1_final_202203 Where Replace(http_referrer, '%', '') like "%/mlb%" or http_referrer like "%account%")

Select device, remote_addr, session, start_time,

count(Case When Replace(http_referrer, '%', '')like "%login%" Then asc_num End) login_attempt,
sum(Case When Replace(http_referrer, '%', '') like "%login%" Then page_duration End) login_duration,
count(Case When Replace(http_referrer, '%', '') like "%create_account%" Then asc_num End) signup_attempt,
sum(Case When Replace(http_referrer, '%', '') like "%create_account%" Then page_duration End) signup_duration,
count(Case When Replace(http_referrer, '%', '') like "%shopping-bag%" Then asc_num End) cart_attempt,
sum(Case When Replace(http_referrer, '%', '') like "%shopping-bag%" Then page_duration End) cart_duration,
count(Case When Replace(http_referrer, '%', '') like "%delivery%" Then asc_num End) delivery_attempt,
sum(Case When Replace(http_referrer, '%', '') like "%delivery%" Then page_duration End) delivery_duration,
count(Case When Replace(http_referrer, '%', '') like "%payment%" Then asc_num End) payment_attempt,
sum(Case When Replace(http_referrer, '%', '') like "%payment%" Then page_duration End) payment_duration,
count(Case When Replace(http_referrer, '%', '') like "%thankyou%" Then asc_num End) checkout_status,
max(Case When desc_num =1 Then http_referrer End) checkout_exit_page,
sum(page_duration) as total_checkout_duration

From TMP
Group by device, remote_addr, session
Order by start_time
;

Insert Into checkout_journey

WITH TMP as (
Select *,
Row_number() over (Partition by session Order by page asc) as asc_num,
Row_number() over (Partition by session Order by page desc) as desc_num

From group1_final_202204 Where Replace(http_referrer, '%', '') like "%/mlb%" or http_referrer like "%account%")

Select device, remote_addr, session, start_time,

count(Case When Replace(http_referrer, '%', '')like "%login%" Then asc_num End) login_attempt,
sum(Case When Replace(http_referrer, '%', '') like "%login%" Then page_duration End) login_duration,
count(Case When Replace(http_referrer, '%', '') like "%create_account%" Then asc_num End) signup_attempt,
sum(Case When Replace(http_referrer, '%', '') like "%create_account%" Then page_duration End) signup_duration,
count(Case When Replace(http_referrer, '%', '') like "%shopping-bag%" Then asc_num End) cart_attempt,
sum(Case When Replace(http_referrer, '%', '') like "%shopping-bag%" Then page_duration End) cart_duration,
count(Case When Replace(http_referrer, '%', '') like "%delivery%" Then asc_num End) delivery_attempt,
sum(Case When Replace(http_referrer, '%', '') like "%delivery%" Then page_duration End) delivery_duration,
count(Case When Replace(http_referrer, '%', '') like "%payment%" Then asc_num End) payment_attempt,
sum(Case When Replace(http_referrer, '%', '') like "%payment%" Then page_duration End) payment_duration,
count(Case When Replace(http_referrer, '%', '') like "%thankyou%" Then asc_num End) checkout_status,
max(Case When desc_num =1 Then http_referrer End) checkout_exit_page,
sum(page_duration) as total_checkout_duration

From TMP
Group by device, remote_addr, session
Order by start_time
;

Insert Into checkout_journey

WITH TMP as (
Select *,
Row_number() over (Partition by session Order by page asc) as asc_num,
Row_number() over (Partition by session Order by page desc) as desc_num

From group1_final_202205 Where Replace(http_referrer, '%', '') like "%/mlb%" or http_referrer like "%account%")

Select device, remote_addr, session, start_time,

count(Case When Replace(http_referrer, '%', '')like "%login%" Then asc_num End) login_attempt,
sum(Case When Replace(http_referrer, '%', '') like "%login%" Then page_duration End) login_duration,
count(Case When Replace(http_referrer, '%', '') like "%create_account%" Then asc_num End) signup_attempt,
sum(Case When Replace(http_referrer, '%', '') like "%create_account%" Then page_duration End) signup_duration,
count(Case When Replace(http_referrer, '%', '') like "%shopping-bag%" Then asc_num End) cart_attempt,
sum(Case When Replace(http_referrer, '%', '') like "%shopping-bag%" Then page_duration End) cart_duration,
count(Case When Replace(http_referrer, '%', '') like "%delivery%" Then asc_num End) delivery_attempt,
sum(Case When Replace(http_referrer, '%', '') like "%delivery%" Then page_duration End) delivery_duration,
count(Case When Replace(http_referrer, '%', '') like "%checkout/payment%" Then asc_num End) payment_attempt,
sum(Case When Replace(http_referrer, '%', '') like "%checkout/payment%" Then page_duration End) payment_duration,
count(Case When Replace(http_referrer, '%', '') like "%thankyou%" Then asc_num End) checkout_status,
max(Case When desc_num =1 Then http_referrer End) checkout_exit_page,
sum(page_duration) as total_checkout_duration

From TMP
Group by device, remote_addr, session
Order by start_time
;
##################################################################################################################################
###3. For Product Journey
Set sql_safe_updates = 0;
Update group1_final_202111
Set page_duration = 0 
Where page_duration is null;

Create Table product_journey
As

With TMP as(

Select *,
row_number() over (Partition by session Order by page_duration asc) asc_num,
row_number() over (Partition by session Order by page_duration desc) desc_num

From group1_final_202111
Where Replace(http_referrer, "%", "") like "%shop%")

Select device, remote_addr, session, start_time,
Count(Case When Replace(http_referrer, "%", "") regexp 'bayswater|darley|lily|softie|alexa|iris|millie|bag|mini|tote|satchel|handle|hobo|small_leather|wallet|card|pouch|purse|case-|accessories|jewelry|bracelet|necklace' 
Then asc_num End) product_count,
Max(Case When desc_num = 1 Then http_referrer End) highest_page,
Max(Case When asc_num = 1 Then http_referrer End) lowest_page,
Max(Case When desc_num =1 Then page_duration End) highest_duration,
Max(Case When asc_num=1 Then page_duration End) lowest_duration,
Sum(page_duration) product_duration

From TMP
Group by device, remote_addr, session
Order by start_time
;

Update group1_final_202112
Set page_duration = 0 
Where page_duration is null;



With TMP as(

Select *,
row_number() over (Partition by session Order by page_duration asc) asc_num,
row_number() over (Partition by session Order by page_duration desc) desc_num

From group1_final_202112
Where Replace(http_referrer, "%", "") like "%shop%")

Select device, remote_addr, session, start_time,
Count(Case When Replace(http_referrer, "%", "") regexp 'bayswater|darley|lily|softie|alexa|iris|millie|bag|mini|tote|satchel|handle|hobo|small_leather|wallet|card|pouch|purse|case-|accessories|jewelry|bracelet|necklace' 
Then asc_num End) product_count,
Max(Case When desc_num = 1 Then http_referrer End) highest_page,
Max(Case When asc_num = 1 Then http_referrer End) lowest_page,
Max(Case When desc_num =1 Then page_duration End) highest_duration,
Max(Case When asc_num=1 Then page_duration End) lowest_duration,
Sum(page_duration) product_duration

From TMP
Group by device, remote_addr, session
Order by start_time
;

Update group1_final_202201
Set page_duration = 0 
Where page_duration is null;

Insert Into product_journey

With TMP as(

Select *,
row_number() over (Partition by session Order by page_duration asc) asc_num,
row_number() over (Partition by session Order by page_duration desc) desc_num

From group1_final_202201
Where Replace(http_referrer, "%", "") like "%shop%")

Select device, remote_addr, session, start_time,
Count(Case When Replace(http_referrer, "%", "") regexp 'bayswater|darley|lily|softie|alexa|iris|millie|bag|mini|tote|satchel|handle|hobo|small_leather|wallet|card|pouch|purse|case-|accessories|jewelry|bracelet|necklace' 
Then asc_num End) product_count,
Max(Case When desc_num = 1 Then http_referrer End) highest_page,
Max(Case When asc_num = 1 Then http_referrer End) lowest_page,
Max(Case When desc_num =1 Then page_duration End) highest_duration,
Max(Case When asc_num=1 Then page_duration End) lowest_duration,
Sum(page_duration) product_duration

From TMP
Group by device, remote_addr, session
Order by start_time
;

Update group1_final_202202
Set page_duration = 0 
Where page_duration is null;

Insert Into product_journey

With TMP as(

Select *,
row_number() over (Partition by session Order by page_duration asc) asc_num,
row_number() over (Partition by session Order by page_duration desc) desc_num

From group1_final_202202
Where Replace(http_referrer, "%", "") like "%shop%")

Select device, remote_addr, session, start_time,
Count(Case When Replace(http_referrer, "%", "") regexp 'bayswater|darley|lily|softie|alexa|iris|millie|bag|mini|tote|satchel|handle|hobo|small_leather|wallet|card|pouch|purse|case-|accessories|jewelry|bracelet|necklace' 
Then asc_num End) product_count,
Max(Case When desc_num = 1 Then http_referrer End) highest_page,
Max(Case When asc_num = 1 Then http_referrer End) lowest_page,
Max(Case When desc_num =1 Then page_duration End) highest_duration,
Max(Case When asc_num=1 Then page_duration End) lowest_duration,
Sum(page_duration) product_duration

From TMP
Group by device, remote_addr, session
Order by start_time
;

Update group1_final_202203
Set page_duration = 0 
Where page_duration is null;

Insert Into product_journey

With TMP as(

Select *,
row_number() over (Partition by session Order by page_duration asc) asc_num,
row_number() over (Partition by session Order by page_duration desc) desc_num

From group1_final_202203
Where Replace(http_referrer, "%", "") like "%shop%")

Select device, remote_addr, session, start_time,
Count(Case When Replace(http_referrer, "%", "") regexp 'bayswater|darley|lily|softie|alexa|iris|millie|bag|mini|tote|satchel|handle|hobo|small_leather|wallet|card|pouch|purse|case-|accessories|jewelry|bracelet|necklace' 
Then asc_num End) product_count,
Max(Case When desc_num = 1 Then http_referrer End) highest_page,
Max(Case When asc_num = 1 Then http_referrer End) lowest_page,
Max(Case When desc_num =1 Then page_duration End) highest_duration,
Max(Case When asc_num=1 Then page_duration End) lowest_duration,
Sum(page_duration) product_duration

From TMP
Group by device, remote_addr, session
Order by start_time
;

Update group1_final_202204
Set page_duration = 0 
Where page_duration is null;

Insert Into product_journey

With TMP as(

Select *,
row_number() over (Partition by session Order by page_duration asc) asc_num,
row_number() over (Partition by session Order by page_duration desc) desc_num

From group1_final_202204
Where Replace(http_referrer, "%", "") like "%shop%")

Select device, remote_addr, session, start_time,
Count(Case When Replace(http_referrer, "%", "") regexp 'bayswater|darley|lily|softie|alexa|iris|millie|bag|mini|tote|satchel|handle|hobo|small_leather|wallet|card|pouch|purse|case-|accessories|jewelry|bracelet|necklace' 
Then asc_num End) product_count,
Max(Case When desc_num = 1 Then http_referrer End) highest_page,
Max(Case When asc_num = 1 Then http_referrer End) lowest_page,
Max(Case When desc_num =1 Then page_duration End) highest_duration,
Max(Case When asc_num=1 Then page_duration End) lowest_duration,
Sum(page_duration) product_duration

From TMP
Group by device, remote_addr, session
Order by start_time
;

Update group1_final_202205
Set page_duration = 0 
Where page_duration is null;

Insert Into product_journey

With TMP as(

Select *,
row_number() over (Partition by session Order by page_duration asc) asc_num,
row_number() over (Partition by session Order by page_duration desc) desc_num

From group1_final_202205
Where Replace(http_referrer, "%", "") like "%shop%")

Select device, remote_addr, session, start_time,
Count(Case When Replace(http_referrer, "%", "") regexp 'bayswater|darley|lily|softie|alexa|iris|millie|bag|mini|tote|satchel|handle|hobo|small_leather|wallet|card|pouch|purse|case-|accessories|jewelry|bracelet|necklace' 
Then asc_num End) product_count,
Max(Case When desc_num = 1 Then http_referrer End) highest_page,
Max(Case When asc_num = 1 Then http_referrer End) lowest_page,
Max(Case When desc_num =1 Then page_duration End) highest_duration,
Max(Case When asc_num=1 Then page_duration End) lowest_duration,
Sum(page_duration) product_duration

From TMP
Group by device, remote_addr, session
Order by start_time
;

###########Left join session_summary_index_total with product_journey by start_time##################
Create Table session_summary_product_journey
As
SELECT A.*, B.product_count, B.highest_page, B.lowest_page, B.highest_duration, B.lowest_duration, B.product_duration
FROM session_summary_index_total A
LEFT JOIN product_journey B ON A.start_time = B.start_time
;

###########################################################################################################################
#4. Summary Statistics
###########################################################################################################################

Select
avg(total_duration) avg_total_duration,
min(total_duration) min_total_duration,
max(total_duration) max_total_duration,
avg(npages) avg_npages,
min(npages) min_npages,
max(npages) max_npages,
avg(avg_duration) avg_avg_duration,
min(avg_duration) min_avg_duration,
max(avg_duration) max_avg_duration

From session_summary_index_total
Where total_duration < 20000
;

Select *
From product_journey
Where product_duration > 3600
;

Select device,
avg(product_count) as avg_product_count,
min(product_count) as min_product_count,
max(product_count) as max_product_count,
avg(lowest_duration) as avg_low_duration,
min(lowest_duration) as min_low_duration,
max(lowest_duration) as max_low_duration,
avg(highest_duration) as avg_high_duration,
min(highest_duration) as min_high_duration,
max(highest_duration) as max_high_duration,
avg(product_duration) as avg_prod_duration,
min(product_duration) as min_prod_duration,
max(product_duration) as max_prod_duration,
avg(product_duration/product_count) as avg_perprod_duration,
min(product_duration/product_count) as min_perprod_duration,
max(product_duration/product_count) as max_perprod_duration

From session_product_journey
Where product_duration < 6000 and product_duration>0
Group by device
;

Select purchased,
count(Case when login_attempt>0 Then session_id End) login_sessions,
avg(Case when login_attempt>0 Then login_attempt End) as avg_login_attempt,
min(Case when login_attempt>0 Then login_attempt End) as min_login_attempt,
max(Case when login_attempt>0 Then login_attempt End) as max_login_attempt,
avg(Case when login_attempt>0 Then login_duration End) as avg_login_duration,
min(Case when login_attempt>0 Then login_duration End) as min_login_duration,
max(Case when login_attempt>0 Then login_duration End) as max_login_duration,
count(Case when signup_attempt>0 Then session_id End) signup_sessions,
avg(Case when signup_attempt>0 Then signup_attempt End) as avg_signup_attempt,
min(Case when signup_attempt>0 Then signup_attempt End) as min_signup_attempt,
max(Case when signup_attempt>0 Then signup_attempt End) as max_signup_attempt,
avg(Case when signup_attempt>0 Then signup_duration End) as avg_signup_duration,
min(Case when signup_attempt>0 Then signup_duration End) as min_signup_duration,
max(Case when signup_attempt>0 Then signup_duration End) as max_signup_duration,
count(Case when cart_attempt>0 Then session_id End) cart_sessions,
avg(Case when cart_attempt>0 Then cart_attempt End) as avg_cart_attempt,
min(Case when cart_attempt>0 Then cart_attempt End) as min_cart_attempt,
max(cart_attempt) as max_cart_attempt,
avg(Case when cart_attempt>0 Then cart_duration End) as avg_cart_duration,
min(Case when cart_attempt>0 Then cart_duration End) as min_cart_duration,
max(Case when cart_attempt>0 Then cart_duration End) as max_cart_duration,
count(Case when delivery_attempt>0 Then session_id End) delivery_sessions,
avg(Case when delivery_attempt>0 Then delivery_attempt End) as avg_delivery_attempt,
min(Case when delivery_attempt>0 Then delivery_attempt End) as min_delivery_attempt,
max(Case when delivery_attempt>0 Then delivery_attempt End) as max_delivery_attempt,
avg(Case when delivery_attempt>0 Then delivery_duration End) as avg_delivery_duration,
min(Case when delivery_attempt>0 Then delivery_duration End) as min_delivery_duration,
max(Case when delivery_attempt>0 Then delivery_duration End) as max_delivery_duration,
count(Case when payment_attempt>0 Then session_id End) payment_sessions,
avg(Case when payment_attempt>0 Then payment_attempt End) as avg_payment_attempt,
min(Case when payment_attempt>0 Then payment_attempt End) as min_payment_attempt,
max(Case when payment_attempt>0 Then payment_attempt End) as max_payment_attempt,
avg(Case when payment_attempt>0 Then payment_duration End) as avg_payment_duration,
min(Case when payment_attempt>0 Then payment_duration End) as min_payment_duration,
max(Case when payment_attempt>0 Then payment_duration End) as max_payment_duration,
avg(total_checkout_duration) as avg_checkout_duration,
min(total_checkout_duration) as min_checkout_duration,
max(total_checkout_duration) as max_checkout_duration

From checkout_journey_with_sessionid
Where total_checkout_duration < 20000
Group by purchased
;
Select count(session_id)
From checkout_journey_with_sessionid
;

Alter Table checkout_journey
Add Column identifier varchar(500)
;
Update checkout_journey
Set identifier = concat(remote_addr, device, start_time)
;
Set sql_safe_updates = 0;
Alter Table session_summary_index_total
Add Column identifier varchar(500)
;
Update session_summary_index_total
Set identifier = concat(remote_addr, device, start_time)
;

Create Table checkout_journey_with_sessionid
As

SELECT A.*, B.session_id
FROM checkout_journey A
LEFT JOIN session_summary_index_total B ON A.identifier = B.identifier
;

Select session_id, count(session_id) From checkout_journey_with_sessionid
Group by session_id;


Alter Table checkout_journey_with_sessionid
Add Column purchased int
;

Update checkout_journey_with_sessionid
Set purchased =1
Where checkout_status >=1
;
Update checkout_journey_with_sessionid
set purchased = 0
Where checkout_status =0 or purchased is null
;

Create Table session_product_journey
As
Select A.*, B.purchased
From session_summary_product_journey A
Left Join checkout_journey_with_sessionid B on A.session_id = B.session_id
;


# Group by Device

Select device, purchased,
avg(total_duration) avg_total_duration,
min(total_duration) min_total_duration,
max(total_duration) max_total_duration,
avg(npages) avg_npages,
min(npages) min_npages,
max(npages) max_npages,
avg(avg_duration) avg_avg_duration,
min(avg_duration) min_avg_duration,
max(avg_duration) max_avg_duration

From session_product_journey
Where total_duration < 20000
Group by device, purchased
;


Select
avg(product_count) as avg_product_count,
min(product_count) as min_product_count,
max(product_count) as max_product_count,
avg(lowest_duration) as avg_low_duration,
min(lowest_duration) as min_low_duration,
max(lowest_duration) as max_low_duration,
avg(highest_duration) as avg_high_duration,
min(highest_duration) as min_high_duration,
max(highest_duration) as max_high_duration,
avg(product_duration) as avg_prod_duration,
min(product_duration) as min_prod_duration,
max(product_duration) as max_prod_duration,
avg(product_duration/product_count) as avg_perprod_duration,
min(product_duration/product_count) as min_perprod_duration,
max(product_duration/product_count) as max_perprod_duration

From product_journey
Where product_duration < 6000 and product_duration>0
;

Create Table group1_final_total
As
Select * From group1_final_202111
;
Insert into group1_final_total
Select * from group1_final_202112
;
Insert into group1_final_total
Select * from group1_final_202201
;
Insert into group1_final_total
Select * from group1_final_202202
;
Insert into group1_final_total
Select * from group1_final_202203
;
Insert into group1_final_total
Select * from group1_final_202204
;
Insert into group1_final_total
Select * from group1_final_202205
;

Set sql_safe_updates = 0;
Alter Table group1_final_total
Add Column identifier Varchar(500)
;
Update group1_final_total
Set identifier = concat(remote_addr, device, start_time) 
;

Select identifier, session, http_referrer, hhmmss, lead_hhmmss, page_duration
From group1_final_total
Where Replace(http_referrer, "%", "") like "%thankyou%"
;
Select session_id, start_page, exit_page, total_duration, npages, identifier
From session_summary_index_total
Where start_page like "%thankyou%" and npages<5
;


Select
count(Case when login_attempt>0 Then session_id End) login,
count(Case when signup_attempt>0 Then session_id End) signup,
count(Case when cart_attempt>0 Then session_id End) cart,
count(Case when delivery_attempt>0 Then session_id End) delivery,
count(Case when payment_attempt>0 Then session_id End) payment

From checkout_journey_with_sessionid
Where total_checkout_duration<20000
Group by purchased
;

Drop table if exists temp;
Create table temp as
Select session_id, identifier
From checkout_journey_with_sessionid
Where Replace(first_page, "%", "") not like "%thankyou%" and purchased =1 and payment_attempt = 0
;

Select * From group1_final_total
Where identifier in (Select identifier from temp)
;

Update checkout_journey_with_sessionid
Set purchased = 0
Where session_id in (Select session_id from temp)
;
Update checkout_journey_with_sessionid
Set purchased = 1
Where Replace(first_page, "%", "") like "%thankyou%"
;
Select * from checkout_journey_with_sessionid
Where Replace(first_page, "%", "") like "%thankyou%"
;

Select * from checkout_journey_with_sessionid
Where delivery_attempt =0 and payment_attempt>0
;
