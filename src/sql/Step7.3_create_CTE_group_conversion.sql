

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

,  user_group_messages as (
    select 
hg.hk_group_id, count(distinct d.message_from) as cnt_users_in_group_with_messages
from STV230530__DWH.h_groups hg 
left join STV230530__STAGING.dialogs d on d.message_group =hg.group_id 
group by hg.hk_group_id
having count(d.message_id)>0
order by cnt_users_in_group_with_messages asc
)


select ugl.hk_group_id,
ugm.cnt_users_in_group_with_messages,
ugl.cnt_added_users,
ugm.cnt_users_in_group_with_messages / ugl.cnt_added_users as group_conversion



from user_group_log as ugl
left join user_group_messages as ugm on ugl.hk_group_id = ugm.hk_group_id
order by ugm.cnt_users_in_group_with_messages / ugl.cnt_added_users desc 
