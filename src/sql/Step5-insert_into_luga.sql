INSERT INTO STV230530__DWH.l_user_group_activity(hk_l_user_group_activity, hk_user_id,hk_group_id,load_dt,load_src)

select distinct
        
	hash(hu.hk_user_id, hg.hk_group_id),
	hu.hk_user_id, 
	hg.hk_group_id,
	now() as load_dt,
	's3' as load_src

from STV230530__STAGING.group_log as gl
left join STV230530__DWH.h_users hu on hu.user_id =gl.user_id 
left join STV230530__DWH.h_groups hg on gl.group_id  = hg.group_id

where hash(hg.hk_group_id,hu.hk_user_id) not in (select hk_l_user_group_activity from STV230530__DWH.l_user_group_activity);
;