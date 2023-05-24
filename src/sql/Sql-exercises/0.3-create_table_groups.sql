
CREATE TABLE STV230530__STAGING.groups
(
    id int NOT NULL,
    admin_id int,
    
    group_name varchar(100),
    registration_dt timestamp,
    is_private boolean,
    CONSTRAINT C_PRIMARY PRIMARY KEY (id) DISABLED
)
 ORDER BY  id, admin_id
SEGMENTED BY hash(groups.id) ALL NODES 
PARTITION BY registration_dt::date
GROUP BY calendar_hierarchy_day(registration_dt::date, 3, 2); 

CREATE PROJECTION STV230530__STAGING.groups_proj1 /*+createtype(L)*/ 
(
 id,
 admin_id,
 group_name,
 registartion_dt,
 is_private
)
AS
 SELECT groups.id,
        groups.admin_id,
        groups.group_name,
        groups.registartion_dt,
        groups.is_private
 FROM STV230530__STAGING.groups
 ORDER BY groups.id, admin_id
SEGMENTED BY hash(groups.id) ALL NODES KSAFE 1;