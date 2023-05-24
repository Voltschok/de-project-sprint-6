-- STV230530.members definition
DROP TABLE STV230530__STAGING.users;
DROP PROJECTION STV230530__STAGING.users_proj1;
CREATE TABLE STV230530__STAGING.users
(
    id int NOT NULL,
 
    chat_name varchar(200),
    registartion_dt timestamp, 
    country varchar(200),
    age int,
    CONSTRAINT C_PRIMARY PRIMARY KEY (id) DISABLED
);


CREATE PROJECTION STV230530__STAGING.users_proj1 /*+createtype(L)*/ 
(
 id,
 chat_name,
 registartion_dt,
 country,
 age 
)
AS
 SELECT users.id,
 		users.chat_name,
        users.registartion_dt,
        users.country,
        users.age
 FROM STV230530__STAGING.users
 ORDER BY users.id
SEGMENTED BY hash(users.id) ALL NODES KSAFE 1;


SELECT MARK_DESIGN_KSAFE(1);