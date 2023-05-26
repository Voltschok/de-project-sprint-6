drop table if exists STV230530__DWH.s_auth_history;

create table STV230530__DWH.s_auth_history
(
hk_l_user_group_activity bigint not null CONSTRAINT fk_s_auth_history_l_user_group_activity 
REFERENCES STV230530__DWH.l_user_group_activity  (hk_l_user_group_activity),
user_id_from int,
event varchar(100),
event_dt datetime,
load_dt datetime,
load_src varchar(20)
)
order by load_dt
SEGMENTED BY hk_l_user_group_activity all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);

INSERT INTO STV230530__DWH.s_auth_history(hk_l_user_group_activity, user_id_from,event,event_dt,load_dt,load_src)

select 
	luga.hk_l_user_group_activity,
	gl.user_id_from,
	gl.event,
	gl.event_dt,
	now() as load_dt,
	's3' as load_src
from STV230530__STAGING.group_log as gl
left join STV230530__DWH.h_groups as hg on gl.group_id = hg.group_id
left join STV230530__DWH.h_users as hu on gl.user_id = hu.user_id
left join STV230530__DWH.l_user_group_activity as luga on hg.hk_group_id = luga.hk_group_id and hu.hk_user_id = luga.hk_user_id

; 
