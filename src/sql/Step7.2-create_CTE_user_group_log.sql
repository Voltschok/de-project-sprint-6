
with tmp as (
select distinct  hu.user_id, luga.hk_user_id, hk_group_id,  sah.event  from STV230530__DWH.l_user_group_activity luga 
left join STV230530__DWH.h_users hu on luga.hk_user_id =hu.hk_user_id 
left join STV230530__DWH.s_auth_history sah on sah.hk_l_user_group_activity =luga.hk_l_user_group_activity 
where sah.event='add' 
), 

user_group_log as (
select * from (
	select count(user_id) as cnt_added_users, hk_group_id from tmp
	group by hk_group_id) t
	where hk_group_id in (select hk_group_id from STV230530__DWH.h_groups order by registration_dt limit 10)
	order by cnt_added_users 
)

select hk_group_id
            ,cnt_added_users
from user_group_log
order by cnt_added_users
limit 10
; 
