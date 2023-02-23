#---------------------------------------------------------
# Work Tables Creation from 202111 to 202205 by Erin
#---------------------------------------------------------

# 1. Generate monthly work-version tables
## Category, campaign, device and temporal variables needed for descriptives created
## All category and campaign variables are binary, default set to "0"

Set innodb_lock_wait_timeout = 99999;
set sql_safe_updates=0;


DELIMITER &&
CREATE PROCEDURE GenWorkVersion(
	IN YYYYMM VARCHAR(10)
)
BEGIN
	SET @YYYYMM = YYYYMM;
    
    # del if exist
    SET @DEL_SCRIPT = CONCAT('DROP TABLE IF EXISTS mulberry.work_KR_web_logs_', @YYYYMM);
    PREPARE dtmt FROM @DEL_SCRIPT;
    EXECUTE dtmt;
    DEALLOCATE PREPARE dtmt;    

	# create the work version table
	SET @GEN_SCRIPT = CONCAT(
    'CREATE TABLE mulberry.work_KR_web_logs_', @YYYYMM, ' (',
    '	id int NOT NULL AUTO_INCREMENT,
		timestamp VARCHAR(50),
		remote_addr VARCHAR(50),
		ip_id BIGINT,
		bytes_sent BIGINT, 
		path VARCHAR(500),
		http_referrer VARCHAR(500),
		http_user_agent VARCHAR(400), 
		jsessionid VARCHAR(100), 
		country VARCHAR(10),
        
        device text,
        date date,
        wkday int,
        time VARCHAR(50),
		men int default 0,
		women int default 0,
		gift int default 0,
		travel int default 0,
		bayswater int default 0,
		darley int default 0,
		lily int default 0,
		softie int default 0,
		bag int default 0,
		small_leather int default 0,
		shoes int default 0,
		accessories int default 0,
		category1 int default 0,
		category2 int default 0,
		category_total int default 0,
		campaign int default 0,
		kakao int default 0,
		naver int default 0, 
		google int default 0,
		youtube int default 0,
		campaign_total int default 0,
 
		PRIMARY KEY (id),
        FULLTEXT idx_url (http_referrer),
        FULLTEXT idx_agent (http_user_agent)
	) DEFAULT CHARSET utf8mb4 COLLATE utf8mb4_unicode_ci ',
    'SELECT
		STR_TO_DATE(SUBSTRING(timestamp, 1, 10),"%Y-%m-%d") date,
        WEEKDAY(STR_TO_DATE(SUBSTRING(timestamp, 1, 10),"%Y-%m-%d")) wkday,
        SUBSTRING(timestamp, 12, 2) time,
		timestamp, remote_addr, 
        cast(conv(substring(md5(remote_addr), 1, 16), 16, 10) as unsigned integer) ip_id,
		bytes_sent, path, http_referrer, http_user_agent, jsessionid, country
	 FROM KR_web_logs_json_', @YYYYMM, ' WHERE country = "KR"; '
    );
    
	PREPARE gtmt FROM @GEN_SCRIPT;
    EXECUTE gtmt;
    DEALLOCATE PREPARE gtmt;    

END &&
DELIMITER ;


Drop Table If Exists work_KR_web_logs_202111;
CALL GenWorkVersion('202111');

Drop Table If Exists work_KR_web_logs_202112;
CALL GenWorkVersion('202112');

Drop Table If Exists work_KR_web_logs_202201;
CALL GenWorkVersion('202201');

Drop Table If Exists work_KR_web_logs_202202;
CALL GenWorkVersion('202202');

Drop Table If Exists work_KR_web_logs_202203;
CALL GenWorkVersion('202203');

Drop Table If Exists work_KR_web_logs_202204;
CALL GenWorkVersion('202204');

Drop Table If Exists work_KR_web_logs_202205;
CALL GenWorkVersion('202205');


# Monthly table updates, ordered chronologically

# Table Updates for 202111
# 2. Set device column values from http_user_agent with index (idx_agent)
Update mulberry.work_KR_web_logs_202111
Set device = "iPad"
Where Match(http_user_agent) Against('iPad' IN BOOLEAN MODE)
;

Update mulberry.work_KR_web_logs_202111
Set device = "android"
Where Match(http_user_agent) Against('Android' IN BOOLEAN MODE)
;

Update mulberry.work_KR_web_logs_202111
Set device = "iPhone"
Where Match(http_user_agent) Against('iPhone' IN BOOLEAN MODE)
;

Update mulberry.work_KR_web_logs_202111
Set device = "macbook"
Where Match(http_user_agent) Against('Macintosh' IN BOOLEAN MODE)
;

Update mulberry.work_KR_web_logs_202111
Set device = "windows"
Where Match(http_user_agent) Against('Windows' IN BOOLEAN MODE)
;

Update mulberry.work_KR_web_logs_202111
Set device = "ubuntu"
Where Match(http_user_agent) Against('Ubuntu' IN BOOLEAN MODE)
;

# 3. Set category variables from http_referrer

Update mulberry.work_KR_web_logs_202111
Set women = 1
Where Match(http_referrer) Against('women' IN BOOLEAN MODE)
;

Update mulberry.work_KR_web_logs_202111
Set men = 1
Where Match(http_referrer) Against('/men' IN BOOLEAN MODE)
;

Update mulberry.work_KR_web_logs_202111
Set gift = 1
Where Match(http_referrer) Against('gift' IN BOOLEAN MODE)
;

Update mulberry.work_KR_web_logs_202111
Set travel = 1
Where Match(http_referrer) Against('travel' IN BOOLEAN MODE)
;

Update mulberry.work_KR_web_logs_202111
Set bayswater = 1
Where Match(http_referrer) Against('bayswater' IN BOOLEAN MODE)
;

Update mulberry.work_KR_web_logs_202111
Set darley = 1
Where Match(http_referrer) Against('darley' IN BOOLEAN MODE)
;

Update mulberry.work_KR_web_logs_202111
Set lily = 1
Where Match(http_referrer) Against('lily' IN BOOLEAN MODE)
;

Update mulberry.work_KR_web_logs_202111
Set softie = 1
Where Match(http_referrer) Against('softie' IN BOOLEAN MODE)
;

Update mulberry.work_KR_web_logs_202111
Set category1 = (women + men + gift + travel + darley + bayswater + lily + softie)
;

Update mulberry.work_KR_web_logs_202111
Set  bag = 1
Where Match(http_referrer) Against('bag mini tote satchel handle hobo' IN BOOLEAN MODE)
;

Update mulberry.work_KR_web_logs_202111
Set small_leather =1
Where Match(http_referrer) Against('small_leather wallet card pouch purse case-holder' IN BOOLEAN MODE)
;

Update mulberry.work_KR_web_logs_202111
Set shoes =1
Where Match(http_referrer) Against('shoes' IN BOOLEAN MODE)
;

Update mulberry.work_KR_web_logs_202111
Set accessories = 1
Where Match(http_referrer) Against('accessories jewelry bracelet necklace' IN BOOLEAN MODE)
;

Update mulberry.work_KR_web_logs_202111
Set category2 = (bag + small_leather + shoes + accessories)
;
#3(2) Sum up total category effects
Update mulberry.work_KR_web_logs_202111
Set category_total = (category1 + category2)
;

# 4. Set category variables from http_referrer
Update mulberry.work_KR_web_logs_202111
Set campaign = 1
Where Match(http_referrer) Against('campaign' IN BOOLEAN MODE)
;

Update mulberry.work_KR_web_logs_202111
Set kakao = 1
Where Match(http_referrer) Against('kakao' IN BOOLEAN MODE)
;

Update mulberry.work_KR_web_logs_202111
Set naver = 1
Where Match(http_referrer) Against('naver' IN BOOLEAN MODE)
;

Update mulberry.work_KR_web_logs_202111
Set google = 1
Where Match(http_referrer) Against('google' IN BOOLEAN MODE)
;

Update mulberry.work_KR_web_logs_202111
Set youtube = 1
Where Match(http_referrer) Against('youtube' IN BOOLEAN MODE)
;

#4(2) Sum up totl campaign effects
Update mulberry.work_KR_web_logs_202111
Set campaign_total = (campaign + naver + kakao + google + youtube)
;

# Table Updates for 202112
Update mulberry.work_KR_web_logs_202112
Set device = "iPad"
Where Match(http_user_agent) Against('iPad' IN BOOLEAN MODE)
;

Update mulberry.work_KR_web_logs_202112
Set device = "android"
Where Match(http_user_agent) Against('Android' IN BOOLEAN MODE)
;

Update mulberry.work_KR_web_logs_202112
Set device = "iPhone"
Where Match(http_user_agent) Against('iPhone' IN BOOLEAN MODE)
;

Update mulberry.work_KR_web_logs_202112
Set device = "macbook"
Where Match(http_user_agent) Against('Macintosh' IN BOOLEAN MODE)
;

Update mulberry.work_KR_web_logs_202112
Set device = "windows"
Where Match(http_user_agent) Against('Windows' IN BOOLEAN MODE)
;

Update mulberry.work_KR_web_logs_202112
Set device = "ubuntu"
Where Match(http_user_agent) Against('Ubuntu' IN BOOLEAN MODE)
;


### http_referrer  to category1 and to category2
Update mulberry.work_KR_web_logs_202112
Set women = 1
Where Match(http_referrer) Against('women' IN BOOLEAN MODE)
;

Update mulberry.work_KR_web_logs_202112
Set men = 1
Where Match(http_referrer) Against('/men' IN BOOLEAN MODE)
;

Update mulberry.work_KR_web_logs_202112
Set gift = 1
Where Match(http_referrer) Against('gift' IN BOOLEAN MODE)
;

Update mulberry.work_KR_web_logs_202112
Set travel = 1
Where Match(http_referrer) Against('travel' IN BOOLEAN MODE)
;

Update mulberry.work_KR_web_logs_202112
Set bayswater = 1
Where Match(http_referrer) Against('bayswater' IN BOOLEAN MODE)
;

Update mulberry.work_KR_web_logs_202112
Set darley = 1
Where Match(http_referrer) Against('darley' IN BOOLEAN MODE)
;

Update mulberry.work_KR_web_logs_202112
Set lily = 1
Where Match(http_referrer) Against('lily' IN BOOLEAN MODE)
;

Update mulberry.work_KR_web_logs_202112
Set softie = 1
Where Match(http_referrer) Against('softie' IN BOOLEAN MODE)
;

Update mulberry.work_KR_web_logs_202112
Set category1 = (women + men + gift + travel + darley + bayswater + lily + softie)
;

Update mulberry.work_KR_web_logs_202112
Set  bag = 1
Where Match(http_referrer) Against('bag mini tote satchel handle hobo' IN BOOLEAN MODE)
;

Update mulberry.work_KR_web_logs_202112
Set small_leather =1
Where Match(http_referrer) Against('small_leather wallet card pouch purse case-holder' IN BOOLEAN MODE)
;

Update mulberry.work_KR_web_logs_202112
Set shoes =1
Where Match(http_referrer) Against('shoes' IN BOOLEAN MODE)
;

Update mulberry.work_KR_web_logs_202112
Set accessories = 1
Where Match(http_referrer) Against('accessories jewelry bracelet necklace' IN BOOLEAN MODE)
;

Update mulberry.work_KR_web_logs_202112
Set category2 = (bag + small_leather + shoes + accessories)
;

Update mulberry.work_KR_web_logs_202112
Set category_total = (category1 + category2)
;

## Campaign direct effect from httlp_referrer
Update mulberry.work_KR_web_logs_202112
Set campaign = 1
Where Match(http_referrer) Against('campaign' IN BOOLEAN MODE)
;

Update mulberry.work_KR_web_logs_202112
Set kakao = 1
Where Match(http_referrer) Against('kakao' IN BOOLEAN MODE)
;

Update mulberry.work_KR_web_logs_202112
Set naver = 1
Where Match(http_referrer) Against('naver' IN BOOLEAN MODE)
;

Update mulberry.work_KR_web_logs_202112
Set google = 1
Where Match(http_referrer) Against('google' IN BOOLEAN MODE)
;

Update mulberry.work_KR_web_logs_202112
Set youtube = 1
Where Match(http_referrer) Against('youtube' IN BOOLEAN MODE)
;

Update mulberry.work_KR_web_logs_202112
Set campaign_total = (campaign + naver + kakao + google + youtube)
;

# Table Updates for 202201
Update mulberry.work_KR_web_logs_202201
Set device = "iPad"
Where Match(http_user_agent) Against('iPad' IN BOOLEAN MODE)
;

Update mulberry.work_KR_web_logs_202201
Set device = "android"
Where Match(http_user_agent) Against('Android' IN BOOLEAN MODE)
;

Update mulberry.work_KR_web_logs_202201
Set device = "iPhone"
Where Match(http_user_agent) Against('iPhone' IN BOOLEAN MODE)
;

Update mulberry.work_KR_web_logs_202201
Set device = "macbook"
Where Match(http_user_agent) Against('Macintosh' IN BOOLEAN MODE)
;

Update mulberry.work_KR_web_logs_202201
Set device = "windows"
Where Match(http_user_agent) Against('Windows' IN BOOLEAN MODE)
;

Update mulberry.work_KR_web_logs_202201
Set device = "ubuntu"
Where Match(http_user_agent) Against('Ubuntu' IN BOOLEAN MODE)
;


### http_referrer  to category1 and to category2
Update mulberry.work_KR_web_logs_202201
Set women = 1
Where Match(http_referrer) Against('women' IN BOOLEAN MODE)
;

Update mulberry.work_KR_web_logs_202201
Set men = 1
Where Match(http_referrer) Against('/men' IN BOOLEAN MODE)
;

Update mulberry.work_KR_web_logs_202201
Set gift = 1
Where Match(http_referrer) Against('gift' IN BOOLEAN MODE)
;

Update mulberry.work_KR_web_logs_202201
Set travel = 1
Where Match(http_referrer) Against('travel' IN BOOLEAN MODE)
;

Update mulberry.work_KR_web_logs_202201
Set bayswater = 1
Where Match(http_referrer) Against('bayswater' IN BOOLEAN MODE)
;

Update mulberry.work_KR_web_logs_202201
Set darley = 1
Where Match(http_referrer) Against('darley' IN BOOLEAN MODE)
;

Update mulberry.work_KR_web_logs_202201
Set lily = 1
Where Match(http_referrer) Against('lily' IN BOOLEAN MODE)
;

Update mulberry.work_KR_web_logs_202201
Set softie = 1
Where Match(http_referrer) Against('softie' IN BOOLEAN MODE)
;

Update mulberry.work_KR_web_logs_202201
Set category1 = (women + men + gift + travel + darley + bayswater + lily + softie)
;

Update mulberry.work_KR_web_logs_202201
Set  bag = 1
Where Match(http_referrer) Against('bag mini tote satchel handle hobo' IN BOOLEAN MODE)
;

Update mulberry.work_KR_web_logs_202201
Set small_leather =1
Where Match(http_referrer) Against('small_leather wallet card pouch purse case-holder' IN BOOLEAN MODE)
;

Update mulberry.work_KR_web_logs_202201
Set shoes =1
Where Match(http_referrer) Against('shoes' IN BOOLEAN MODE)
;

Update mulberry.work_KR_web_logs_202201
Set accessories = 1
Where Match(http_referrer) Against('accessories jewelry bracelet necklace' IN BOOLEAN MODE)
;

Update mulberry.work_KR_web_logs_202201
Set category2 = (bag + small_leather + shoes + accessories)
;

Update mulberry.work_KR_web_logs_202201
Set category_total = (category1 + category2)
;

## Campaign direct effect from httlp_referrer
Update mulberry.work_KR_web_logs_202201
Set campaign = 1
Where Match(http_referrer) Against('campaign' IN BOOLEAN MODE)
;

Update mulberry.work_KR_web_logs_202201
Set kakao = 1
Where Match(http_referrer) Against('kakao' IN BOOLEAN MODE)
;

Update mulberry.work_KR_web_logs_202201
Set naver = 1
Where Match(http_referrer) Against('naver' IN BOOLEAN MODE)
;

Update mulberry.work_KR_web_logs_202201
Set google = 1
Where Match(http_referrer) Against('google' IN BOOLEAN MODE)
;

Update mulberry.work_KR_web_logs_202201
Set youtube = 1
Where Match(http_referrer) Against('youtube' IN BOOLEAN MODE)
;

Update mulberry.work_KR_web_logs_202201
Set campaign_total = (campaign + naver + kakao + google + youtube)
;

# Table Updates for 202202
Update mulberry.work_KR_web_logs_202202
Set device = "iPad"
Where Match(http_user_agent) Against('iPad' IN BOOLEAN MODE)
;

Update mulberry.work_KR_web_logs_202202
Set device = "android"
Where Match(http_user_agent) Against('Android' IN BOOLEAN MODE)
;

Update mulberry.work_KR_web_logs_202202
Set device = "iPhone"
Where Match(http_user_agent) Against('iPhone' IN BOOLEAN MODE)
;

Update mulberry.work_KR_web_logs_202202
Set device = "macbook"
Where Match(http_user_agent) Against('Macintosh' IN BOOLEAN MODE)
;

Update mulberry.work_KR_web_logs_202202
Set device = "windows"
Where Match(http_user_agent) Against('Windows' IN BOOLEAN MODE)
;

Update mulberry.work_KR_web_logs_202202
Set device = "ubuntu"
Where Match(http_user_agent) Against('Ubuntu' IN BOOLEAN MODE)
;


### http_referrer  to category1 and to category2
Update mulberry.work_KR_web_logs_202202
Set women = 1
Where Match(http_referrer) Against('women' IN BOOLEAN MODE)
;

Update mulberry.work_KR_web_logs_202202
Set men = 1
Where Match(http_referrer) Against('/men' IN BOOLEAN MODE)
;

Update mulberry.work_KR_web_logs_202202
Set gift = 1
Where Match(http_referrer) Against('gift' IN BOOLEAN MODE)
;

Update mulberry.work_KR_web_logs_202202
Set travel = 1
Where Match(http_referrer) Against('travel' IN BOOLEAN MODE)
;

Update mulberry.work_KR_web_logs_202202
Set bayswater = 1
Where Match(http_referrer) Against('bayswater' IN BOOLEAN MODE)
;

Update mulberry.work_KR_web_logs_202202
Set darley = 1
Where Match(http_referrer) Against('darley' IN BOOLEAN MODE)
;

Update mulberry.work_KR_web_logs_202202
Set lily = 1
Where Match(http_referrer) Against('lily' IN BOOLEAN MODE)
;

Update mulberry.work_KR_web_logs_202202
Set softie = 1
Where Match(http_referrer) Against('softie' IN BOOLEAN MODE)
;

Update mulberry.work_KR_web_logs_202202
Set category1 = (women + men + gift + travel + darley + bayswater + lily + softie)
;

Update mulberry.work_KR_web_logs_202202
Set  bag = 1
Where Match(http_referrer) Against('bag mini tote satchel handle hobo' IN BOOLEAN MODE)
;

Update mulberry.work_KR_web_logs_202202
Set small_leather =1
Where Match(http_referrer) Against('small_leather wallet card pouch purse case-holder' IN BOOLEAN MODE)
;

Update mulberry.work_KR_web_logs_202202
Set shoes =1
Where Match(http_referrer) Against('shoes' IN BOOLEAN MODE)
;

Update mulberry.work_KR_web_logs_202202
Set accessories = 1
Where Match(http_referrer) Against('accessories jewelry bracelet necklace' IN BOOLEAN MODE)
;

Update mulberry.work_KR_web_logs_202202
Set category2 = (bag + small_leather + shoes + accessories)
;

Update mulberry.work_KR_web_logs_202202
Set category_total = (category1 + category2)
;

## Campaign direct effect from httlp_referrer
Update mulberry.work_KR_web_logs_202202
Set campaign = 1
Where Match(http_referrer) Against('campaign' IN BOOLEAN MODE)
;

Update mulberry.work_KR_web_logs_202202
Set kakao = 1
Where Match(http_referrer) Against('kakao' IN BOOLEAN MODE)
;

Update mulberry.work_KR_web_logs_202202
Set naver = 1
Where Match(http_referrer) Against('naver' IN BOOLEAN MODE)
;

Update mulberry.work_KR_web_logs_202202
Set google = 1
Where Match(http_referrer) Against('google' IN BOOLEAN MODE)
;

Update mulberry.work_KR_web_logs_202202
Set youtube = 1
Where Match(http_referrer) Against('youtube' IN BOOLEAN MODE)
;

Update mulberry.work_KR_web_logs_202202
Set campaign_total = (campaign + naver + kakao + google + youtube)
;

# Table Updates for 202203
Update mulberry.work_KR_web_logs_202203
Set device = "iPad"
Where Match(http_user_agent) Against('iPad' IN BOOLEAN MODE)
;

Update mulberry.work_KR_web_logs_202203
Set device = "android"
Where Match(http_user_agent) Against('Android' IN BOOLEAN MODE)
;

Update mulberry.work_KR_web_logs_202203
Set device = "iPhone"
Where Match(http_user_agent) Against('iPhone' IN BOOLEAN MODE)
;

Update mulberry.work_KR_web_logs_202203
Set device = "macbook"
Where Match(http_user_agent) Against('Macintosh' IN BOOLEAN MODE)
;

Update mulberry.work_KR_web_logs_202203
Set device = "windows"
Where Match(http_user_agent) Against('Windows' IN BOOLEAN MODE)
;

Update mulberry.work_KR_web_logs_202203
Set device = "ubuntu"
Where Match(http_user_agent) Against('Ubuntu' IN BOOLEAN MODE)
;


### http_referrer  to category1 and to category2
Update mulberry.work_KR_web_logs_202203
Set women = 1
Where Match(http_referrer) Against('women' IN BOOLEAN MODE)
;

Update mulberry.work_KR_web_logs_202203
Set men = 1
Where Match(http_referrer) Against('/men' IN BOOLEAN MODE)
;

Update mulberry.work_KR_web_logs_202203
Set gift = 1
Where Match(http_referrer) Against('gift' IN BOOLEAN MODE)
;

Update mulberry.work_KR_web_logs_202203
Set travel = 1
Where Match(http_referrer) Against('travel' IN BOOLEAN MODE)
;

Update mulberry.work_KR_web_logs_202203
Set bayswater = 1
Where Match(http_referrer) Against('bayswater' IN BOOLEAN MODE)
;

Update mulberry.work_KR_web_logs_202203
Set darley = 1
Where Match(http_referrer) Against('darley' IN BOOLEAN MODE)
;

Update mulberry.work_KR_web_logs_202203
Set lily = 1
Where Match(http_referrer) Against('lily' IN BOOLEAN MODE)
;

Update mulberry.work_KR_web_logs_202203
Set softie = 1
Where Match(http_referrer) Against('softie' IN BOOLEAN MODE)
;

Update mulberry.work_KR_web_logs_202203
Set category1 = (women + men + gift + travel + darley + bayswater + lily + softie)
;

Update mulberry.work_KR_web_logs_202203
Set  bag = 1
Where Match(http_referrer) Against('bag mini tote satchel handle hobo' IN BOOLEAN MODE)
;

Update mulberry.work_KR_web_logs_202203
Set small_leather =1
Where Match(http_referrer) Against('small_leather wallet card pouch purse case-holder' IN BOOLEAN MODE)
;

Update mulberry.work_KR_web_logs_202203
Set shoes =1
Where Match(http_referrer) Against('shoes' IN BOOLEAN MODE)
;

Update mulberry.work_KR_web_logs_202203
Set accessories = 1
Where Match(http_referrer) Against('accessories jewelry bracelet necklace' IN BOOLEAN MODE)
;

Update mulberry.work_KR_web_logs_202203
Set category2 = (bag + small_leather + shoes + accessories)
;

Update mulberry.work_KR_web_logs_202203
Set category_total = (category1 + category2)
;

## Campaign direct effect from httlp_referrer
Update mulberry.work_KR_web_logs_202203
Set campaign = 1
Where Match(http_referrer) Against('campaign' IN BOOLEAN MODE)
;

Update mulberry.work_KR_web_logs_202203
Set kakao = 1
Where Match(http_referrer) Against('kakao' IN BOOLEAN MODE)
;

Update mulberry.work_KR_web_logs_202203
Set naver = 1
Where Match(http_referrer) Against('naver' IN BOOLEAN MODE)
;

Update mulberry.work_KR_web_logs_202203
Set google = 1
Where Match(http_referrer) Against('google' IN BOOLEAN MODE)
;

Update mulberry.work_KR_web_logs_202203
Set youtube = 1
Where Match(http_referrer) Against('youtube' IN BOOLEAN MODE)
;

Update mulberry.work_KR_web_logs_202203
Set campaign_total = (campaign + naver + kakao + google + youtube)
;

# Table Updates for 202204
Update mulberry.work_KR_web_logs_202204
Set device = "iPad"
Where Match(http_user_agent) Against('iPad' IN BOOLEAN MODE)
;

Update mulberry.work_KR_web_logs_202204
Set device = "android"
Where Match(http_user_agent) Against('Android' IN BOOLEAN MODE)
;

Update mulberry.work_KR_web_logs_202204
Set device = "iPhone"
Where Match(http_user_agent) Against('iPhone' IN BOOLEAN MODE)
;

Update mulberry.work_KR_web_logs_202204
Set device = "macbook"
Where Match(http_user_agent) Against('Macintosh' IN BOOLEAN MODE)
;

Update mulberry.work_KR_web_logs_202204
Set device = "windows"
Where Match(http_user_agent) Against('Windows' IN BOOLEAN MODE)
;

Update mulberry.work_KR_web_logs_202204
Set device = "ubuntu"
Where Match(http_user_agent) Against('Ubuntu' IN BOOLEAN MODE)
;


### http_referrer  to category1 and to category2
Update mulberry.work_KR_web_logs_202204
Set women = 1
Where Match(http_referrer) Against('women' IN BOOLEAN MODE)
;

Update mulberry.work_KR_web_logs_202204
Set men = 1
Where Match(http_referrer) Against('/men' IN BOOLEAN MODE)
;

Update mulberry.work_KR_web_logs_202204
Set gift = 1
Where Match(http_referrer) Against('gift' IN BOOLEAN MODE)
;

Update mulberry.work_KR_web_logs_202204
Set travel = 1
Where Match(http_referrer) Against('travel' IN BOOLEAN MODE)
;

Update mulberry.work_KR_web_logs_202204
Set bayswater = 1
Where Match(http_referrer) Against('bayswater' IN BOOLEAN MODE)
;

Update mulberry.work_KR_web_logs_202204
Set darley = 1
Where Match(http_referrer) Against('darley' IN BOOLEAN MODE)
;

Update mulberry.work_KR_web_logs_202204
Set lily = 1
Where Match(http_referrer) Against('lily' IN BOOLEAN MODE)
;

Update mulberry.work_KR_web_logs_202204
Set softie = 1
Where Match(http_referrer) Against('softie' IN BOOLEAN MODE)
;

Update mulberry.work_KR_web_logs_202204
Set category1 = (women + men + gift + travel + darley + bayswater + lily + softie)
;

Update mulberry.work_KR_web_logs_202204
Set  bag = 1
Where Match(http_referrer) Against('bag mini tote satchel handle hobo' IN BOOLEAN MODE)
;

Update mulberry.work_KR_web_logs_202204
Set small_leather = 1
Where Match(http_referrer) Against('small_leather wallet card pouch purse case-holder' IN BOOLEAN MODE)
;

Update mulberry.work_KR_web_logs_202204
Set shoes =1
Where Match(http_referrer) Against('shoes' IN BOOLEAN MODE)
;

Update mulberry.work_KR_web_logs_202204
Set accessories = 1
Where Match(http_referrer) Against('accessories jewelry bracelet necklace' IN BOOLEAN MODE)
;

Update mulberry.work_KR_web_logs_202204
Set category2 = (bag + small_leather + shoes + accessories)
;

Update mulberry.work_KR_web_logs_202204
Set category_total = (category1 + category2)
;

## Campaign direct effect from httlp_referrer
Update mulberry.work_KR_web_logs_202204
Set campaign = 1
Where Match(http_referrer) Against('campaign' IN BOOLEAN MODE)
;

Update mulberry.work_KR_web_logs_202204
Set kakao = 1
Where Match(http_referrer) Against('kakao' IN BOOLEAN MODE)
;

Update mulberry.work_KR_web_logs_202204
Set naver = 1
Where Match(http_referrer) Against('naver' IN BOOLEAN MODE)
;

Update mulberry.work_KR_web_logs_202204
Set google = 1
Where Match(http_referrer) Against('google' IN BOOLEAN MODE)
;

Update mulberry.work_KR_web_logs_202204
Set youtube = 1
Where Match(http_referrer) Against('youtube' IN BOOLEAN MODE)
;

Update mulberry.work_KR_web_logs_202204
Set campaign_total = (campaign + naver + kakao + google + youtube)
;

# Table Updates for 202205
Update mulberry.work_KR_web_logs_202205
Set device = "iPad"
Where Match(http_user_agent) Against('iPad' IN BOOLEAN MODE)
;

Update mulberry.work_KR_web_logs_202205
Set device = "android"
Where Match(http_user_agent) Against('Android' IN BOOLEAN MODE)
;

Update mulberry.work_KR_web_logs_202205
Set device = "iPhone"
Where Match(http_user_agent) Against('iPhone' IN BOOLEAN MODE)
;

Update mulberry.work_KR_web_logs_202205
Set device = "macbook"
Where Match(http_user_agent) Against('Macintosh' IN BOOLEAN MODE)
;

Update mulberry.work_KR_web_logs_202205
Set device = "windows"
Where Match(http_user_agent) Against('Windows' IN BOOLEAN MODE)
;

Update mulberry.work_KR_web_logs_202205
Set device = "ubuntu"
Where Match(http_user_agent) Against('Ubuntu' IN BOOLEAN MODE)
;


### http_referrer  to category1 and to category2
Update mulberry.work_KR_web_logs_202205
Set women = 1
Where Match(http_referrer) Against('women' IN BOOLEAN MODE)
;

Update mulberry.work_KR_web_logs_202205
Set men = 1
Where Match(http_referrer) Against('/men' IN BOOLEAN MODE)
;

Update mulberry.work_KR_web_logs_202205
Set gift = 1
Where Match(http_referrer) Against('gift' IN BOOLEAN MODE)
;

Update mulberry.work_KR_web_logs_202205
Set travel = 1
Where Match(http_referrer) Against('travel' IN BOOLEAN MODE)
;

Update mulberry.work_KR_web_logs_202205
Set bayswater = 1
Where Match(http_referrer) Against('bayswater' IN BOOLEAN MODE)
;

Update mulberry.work_KR_web_logs_202205
Set darley = 1
Where Match(http_referrer) Against('darley' IN BOOLEAN MODE)
;

Update mulberry.work_KR_web_logs_202205
Set lily = 1
Where Match(http_referrer) Against('lily' IN BOOLEAN MODE)
;

Update mulberry.work_KR_web_logs_202205
Set softie = 1
Where Match(http_referrer) Against('softie' IN BOOLEAN MODE)
;

Update mulberry.work_KR_web_logs_202205
Set category1 = (women + men + gift + travel + darley + bayswater + lily + softie)
;

Update mulberry.work_KR_web_logs_202205
Set  bag =0;
Update mulberry.work_KR_web_logs_202205
Set  bag = 1
Where Match(http_referrer) Against('bag mini tote satchel handle hobo' IN BOOLEAN MODE)
;
Update mulberry.work_KR_web_logs_202205
Set small_leather =0;
Update mulberry.work_KR_web_logs_202205
Set small_leather =1
Where Match(http_referrer) Against('small_leather wallet card pouch purse case-holder' IN BOOLEAN MODE)
;
Update mulberry.work_KR_web_logs_202205
Set shoes =0;
Update mulberry.work_KR_web_logs_202205
Set shoes =1
Where Match(http_referrer) Against('shoes' IN BOOLEAN MODE)
;
Update mulberry.work_KR_web_logs_202205
Set accessories = 0;
Update mulberry.work_KR_web_logs_202205
Set accessories = 1
Where Match(http_referrer) Against('accessories jewelry bracelet necklace' IN BOOLEAN MODE)
;

Update mulberry.work_KR_web_logs_202205
Set category2 = (bag + small_leather + shoes + accessories)
;

Update mulberry.work_KR_web_logs_202205
Set category_total = (category1 + category2)
;

## Campaign direct effect from httlp_referrer

Update mulberry.work_KR_web_logs_202205
Set campaign = 1
Where Match(http_referrer) Against('campaign' IN BOOLEAN MODE)
;

Update mulberry.work_KR_web_logs_202205
Set kakao = 1
Where Match(http_referrer) Against('kakao' IN BOOLEAN MODE)
;

Update mulberry.work_KR_web_logs_202205
Set naver = 1
Where Match(http_referrer) Against('naver' IN BOOLEAN MODE)
;

Update mulberry.work_KR_web_logs_202205
Set google = 1
Where Match(http_referrer) Against('google' IN BOOLEAN MODE)
;

Update mulberry.work_KR_web_logs_202205
Set youtube = 1
Where Match(http_referrer) Against('youtube' IN BOOLEAN MODE)
;

Update mulberry.work_KR_web_logs_202205
Set campaign_total = (campaign + naver + kakao + google + youtube)
;


#########################End####################################

