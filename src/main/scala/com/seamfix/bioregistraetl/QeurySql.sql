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



CREATE or replace view visualization_db.org_sub AS
SELECT DISTINCT o.name,
         o."createdby" email,
         u."planname", o.subscriptiontype,
         from_unixtime((o."created"/1000)) AS sign_up_date,
         from_unixtime((cast(s."startdate" AS BIGINT)/1000)) subcription_date,
         s.amountpaid,
         s.orgid,
         s.active,
         s.status,
         s.subscriptioncycle,
         s.paymentmode,
         s.transactionref
         --c.count captured_records
FROM organizations o
LEFT JOIN subscription_history s
    ON o."orgid" = s."orgid"
LEFT JOIN user_billing_band u
    ON s."subscriptionplan" = u._id
 ---left JOIN count_cr__ c ON c."orgid" = o."orgid"
 ---where date(from_unixtime((o."created"/1000))) >= '2019-01-01'
 ---where lower(u."planname") is NOT null;


CREATE view visualization_db.project_view AS
SELECT orgid,
         pid,
        useid,
         active,
         from_unixtime((created/1000)) AS created,
         from_unixtime((startdate/1000)) AS startdate,
         from_unixtime((enddate/1000)) AS enddate,
         category,
         name AS projectname
FROM projects;

SELECT *
FROM visualization_db.org_sub limit 50;

SELECT *
FROM
    (SELECT *
    FROM project_per_org_view --558
    union ALL
        (SELECT o.orgid,
         NULL AS projects_done,
         NULL AS first_project,
         max(sign_up_date),
         max(sign_up_date),
         name
        FROM org_sub o
        WHERE o.orgid NOT IN
            (SELECT orgid
            FROM project_per_org_view)
            GROUP BY  o.orgid, o.name))
        WHERE projects_done is null; -- 1230


    SELECT count(distinct orgid)
FROM org_sub -- 1788
 per revord per user;

 SELECT *
FROM organizations limit 50


	create view churn_kpi as
	select count(*) as num, 'TOTAL SIGNUPS' as kpi
	from visualization_db.project_date --where projects_done is null
	UNION
	select count(*) , 'CREATED PROJECTS' from visualization_db.project_date
	WHERE PROJECTS_DONE > 0
	UNION
	select count(*) , 'CONVERTED' from visualization_db.project_date
	WHERE PROJECTS_DONE > 0
	AND orgid in (select orgid from app_db_p.subscription_history group by orgid having  count (orgid) > 1)


	Churn Since Inception
	select  (cast ((a.num - b.num) as DOUBLE)/ cast(a.num  as DOUBLE )) as n, 'Churn Rate'
    from ((select * from visualization_db.churn_kpi

CREATE or replace view visualization_db.org_status as
select orgid, subscriptionstatus, min(sign_up_date) as sign_up_date, 
min(subscription_date) as trial_date, max(subscription_end_date) as last_date from visualization_db.org_sub
group by orgid, subscriptionstatus