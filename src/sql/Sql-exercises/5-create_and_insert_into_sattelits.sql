drop table if exists STV230530__DWH.s_admins;

create table STV230530__DWH.s_admins
(
hk_admin_id bigint not null CONSTRAINT fk_s_admins_l_admins REFERENCES STV230530__DWH.l_admins (hk_l_admin_id),
is_admin boolean,
admin_from datetime,
load_dt datetime,
load_src varchar(20)
)
order by load_dt
SEGMENTED BY hk_admin_id all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);


INSERT INTO STV230530__DWH.s_admins(hk_admin_id, is_admin,admin_from,load_dt,load_src)
select la.hk_l_admin_id,
True as is_admin,
hg.registration_dt,
now() as load_dt,
's3' as load_src
from STV230530__DWH.l_admins as la
left join STV230530__DWH.h_groups as hg on la.hk_group_id = hg.hk_group_id;


drop table if exists STV230530__DWH.s_user_socdem;

create table STV230530__DWH.s_user_socdem
(
hk_user_id bigint not null CONSTRAINT fk_s_user_socdem_h_users REFERENCES STV230530__DWH.h_users (hk_user_id),
--chat_name varchar(200),
country varchar(200),
age int,
load_dt datetime,
load_src varchar(20)
)
order by load_dt
SEGMENTED BY hk_user_id all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);


INSERT INTO STV230530__DWH.s_user_socdem(hk_user_id, country,age,load_dt,load_src)
select hu.hk_user_id,
--u.chat_name,
u.country,
u.age,
now() as load_dt,
's3' as load_src
from STV230530__DWH.h_users as hu
left join STV230530__STAGING.users as u on hu.user_id = u.id;




drop table if exists STV230530__DWH.s_group_name;

create table STV230530__DWH.s_group_name
(
hk_group_id bigint not null CONSTRAINT fk_s_group_name_h_groups REFERENCES STV230530__DWH.h_groups (hk_group_id),
group_name varchar(200),
load_dt datetime,
load_src varchar(20)
)
order by load_dt
SEGMENTED BY hk_group_id all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);


INSERT INTO STV230530__DWH.s_group_name(hk_group_id, group_name ,load_dt,load_src)
select hg.hk_group_id,
g.group_name,
now() as load_dt,
's3' as load_src
from STV230530__DWH.h_groups as hg
 left join STV230530__STAGING.groups as g on g.id =hg.group_id;


drop table if exists STV230530__DWH.s_group_private_status;

create table STV230530__DWH.s_group_private_status
(
hk_group_id bigint not null CONSTRAINT fk_s_group_private_status_h_groups REFERENCES STV230530__DWH.h_groups (hk_group_id),
is_private boolean,
load_dt datetime,
load_src varchar(20)
)
order by load_dt
SEGMENTED BY hk_group_id all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);


INSERT INTO STV230530__DWH.s_group_private_status(hk_group_id, is_private ,load_dt,load_src)
select hg.hk_group_id,
g.is_private ,
now() as load_dt,
's3' as load_src
from STV230530__DWH.h_groups as hg
 left join STV230530__STAGING.groups as g on g.id =hg.group_id;

drop table if exists STV230530__DWH.s_user_chatinfo;

create table STV230530__DWH.s_user_chatinfo
(
hk_user_id bigint not null CONSTRAINT fk_s_user_chatinfo_h_users REFERENCES STV230530__DWH.h_users (hk_user_id),
chat_name varchar(200),

load_dt datetime,
load_src varchar(20)
)
order by load_dt
SEGMENTED BY hk_user_id all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);


INSERT INTO STV230530__DWH.s_user_chatinfo(hk_user_id, chat_name,load_dt,load_src)
select hu.hk_user_id,
u.chat_name,

now() as load_dt,
's3' as load_src
from STV230530__DWH.h_users as hu
left join STV230530__STAGING.users as u on hu.user_id = u.id;



drop table if exists STV230530__DWH.s_dialog_info;

create table STV230530__DWH.s_dialog_info
(
hk_message_id bigint not null CONSTRAINT fk_s_dialog_info_h_users REFERENCES STV230530__DWH.h_dialogs (hk_message_id),
message varchar(2000),
message_from varchar(200),
message_to varchar(200),
load_dt datetime,
load_src varchar(20)
)
order by load_dt
SEGMENTED BY hk_message_id all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);


INSERT INTO STV230530__DWH.s_dialog_info(hk_message_id, message,message_from,message_to, load_dt,load_src)
select hd.hk_message_id,
d.message,
d.message_from,
d.message_to,

now() as load_dt,
's3' as load_src
from STV230530__DWH.h_dialogs as hd
left join STV230530__STAGING.dialogs as d on hd.message_id = d.message_id;


























