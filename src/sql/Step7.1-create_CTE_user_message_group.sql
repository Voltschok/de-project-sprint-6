with user_group_messages as (
    select 
hg.hk_group_id, count(distinct d.message_from) as cnt_users_in_group_with_messages
from STV230530__DWH.h_groups hg 
left join STV230530__STAGING.dialogs d on d.message_group =hg.group_id 
group by hg.hk_group_id
having count(d.message_id)>0
order by cnt_users_in_group_with_messages asc
)

select hk_group_id,
            cnt_users_in_group_with_messages
from user_group_messages
order by cnt_users_in_group_with_messages
limit 10;


--select * from STV230530__DWH.h_groups hg where hg.hk_group_id =6210160447255813510;
--select * from STV230530__STAGING.dialogs WHERE message_group=188;

