select p.orgid, count(distinct p.pid) as projects_done, min(p.created) as first_project,
max(o.sign_up_date) as signed_up,min(o.sign_up_date) as signed_up_2, o.name
from org_sub o join project_view p
on o.orgid = p.orgid
group by p.orgid, o.name
order by 3 asc;

select * from subscription_plan;

SELECT *, split_part(subscriptionplanid, '-', 1)  as subscriptionType from subscription_plan

select * from app_db_p.subscription_payment_history;

select * from app_db_p.subscription_history;


CREATE OR REPLACE VIEW "project_per_org_view" AS
select p.orgid, count(distinct p.pid) as projects_done, min(p.created) as first_project,
max(o.sign_up_date) as signed_up,min(o.sign_up_date) as signed_up_2, o.name
from org_sub o join project_view p
on o.orgid = p.orgid
group by p.orgid, o.name
order by 3 asc;

select * from project_per_org_view
union all
(select o.orgid, null as projects_done, null as first_project,
max(sign_up_date), max(sign_up_date), name from org_sub o
where o.orgid not in (select orgid from project_per_org_view)
group by o.orgid, o.name);

CREATE OR REPLACE VIEW "project_date" AS
select * from project_per_org_view
union all
(select o.orgid, null as projects_done, null as first_project,
max(sign_up_date), max(sign_up_date), name from org_sub o
where o.orgid not in (select orgid from project_per_org_view)
group by o.orgid, o.name);

select *, date_diff('day', signed_up, first_project) as days_first_project
from project_date
