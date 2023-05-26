
CREATE TABLE STV230530__STAGING.group_log
(
    group_id int NOT NULL,
    user_id int,
    
    user_id_from int,
    event varchar(100),
    event_dt timestamp,
  
    CONSTRAINT C_PRIMARY PRIMARY KEY (group_id,user_id ) DISABLED
)
 ORDER BY  group_id, user_id
SEGMENTED BY hash(group_log.group_id, group_log.user_id) ALL NODES 
PARTITION BY event_dt::date
GROUP BY calendar_hierarchy_day(event_dt::date, 3, 2); 

CREATE PROJECTION STV230530__STAGING.group_log_proj1 /*+createtype(L)*/ 
(
 group_id,
 user_id,
 user_id_from,
 event,
 event_dt
)
AS
 SELECT group_log.group_id,
        group_log.user_id,
        group_log.user_id_from,
        group_log.event,
        group_log.event_dt
 FROM STV230530__STAGING.group_log
 ORDER BY group_log.group_id, user_id
SEGMENTED BY hash(group_log.group_id, group_log.user_id) ALL NODES KSAFE 1;


