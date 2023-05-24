drop table STV230530__STAGING.dialogs cascade;
CREATE TABLE STV230530__STAGING.dialogs
(
    message_id int NOT NULL,
    message_ts timestamp ,
    message_from int,
    message_to int,
    message varchar(1000),
    message_group int,
    CONSTRAINT C_PRIMARY PRIMARY KEY (message_id) DISABLED
)


ORDER BY  message_id
SEGMENTED BY hash(message_id) ALL NODES 
PARTITION BY message_ts::date
GROUP BY calendar_hierarchy_day(message_ts::date, 3, 2);




CREATE PROJECTION STV230530__STAGING.dialogs_proj1  
(
 message_id,
 message_ts,
 message_from,
 message_to,
 message,
 message_group
)
AS
 SELECT dialogs.message_id,
        dialogs.message_ts,
        dialogs.message_from,
        dialogs.message_to,
        dialogs.message,
        dialogs.message_group
 FROM STV230530__STAGING.dialogs
SELECT MARK_DESIGN_KSAFE(1);