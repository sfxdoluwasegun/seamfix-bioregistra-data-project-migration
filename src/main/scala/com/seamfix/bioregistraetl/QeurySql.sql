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


CREATE OR REPLACE VIEW visualization_db.org_sub AS
SELECT DISTINCT
  "o"."name"
, "o"."orgid" "organization_id"
, "o"."createdby" "email"
, "u"."planname"
, "o"."subscriptiontype"
, "o"."orgtype" "organization_type"
, "o"."subscriptionstatus"
, "from_unixtime"(("o"."created" / 1000)) "sign_up_date"
, "from_unixtime"((CAST("s"."startdate" AS bigint) / 1000)) "subscription_date"
, "from_unixtime"((CAST("s"."enddate" AS bigint) / 1000)) "subscription_end_date"
, "s"."amountpaid"
, "s"."orgid"
, "s"."active"
, "s"."status"
, "s"."subscriptioncycle"
, "s"."paymentmode"
, "s"."transactionref"
FROM
  ((app_db_p.organizations o
LEFT JOIN app_db_p.subscription_history s ON ("o"."orgid" = "s"."orgid"))
LEFT JOIN app_db_p.user_billing_band u ON ("s"."subscriptionplan" = "u"."_id"))


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


----Churn Since Inception
	select  (cast ((a.num - b.num) as DOUBLE)/ cast(a.num  as DOUBLE )) as n, 'Churn Rate'
    from ((select * from visualization_db.churn_kpi
	where kpi = 'TOTAL SIGNUPS') a cross join (select * from  visualization_db.churn_kpi
	where kpi = 'CONVERTED') b);





create view visualization_db.customer_journey_count as
	select count(*) as num, 'TOTAL SIGNUPS' as kpi
	from visualization_db.project_date where  format_datetime (signed_up, 'y') = '2019'
	UNION
	select count(*) , 'CREATED PROJECTS' from visualization_db.project_date
	WHERE PROJECTS_DONE > 0 and format_datetime (signed_up, 'y') = '2019'
	UNION
	select count(*) , 'CONVERTED' from visualization_db.project_date vp
WHERE PROJECTS_DONE > 0 AND format_datetime (signed_up, 'y') = '2019'
AND orgid in (select orgid from app_db_p.subscription_history group by orgid having count (orgid) > 1)
UNION
select count(*), 'RENEWED SUBSCRIPTION' from visualization_db.project_date vp
WHERE PROJECTS_DONE > 0 AND format_datetime (signed_up, 'y') = '2019'
AND orgid in (select orgid from app_db_p.subscription_history group by orgid having  count (orgid) > 2)
UNION
select COUNT(DISTINCT orgid), 'CURRENTLY ACTIVE'from app_db_p.subscription_history where status != 'EXPIRED'


CREATE OR REPLACE VIEW "project_date" AS
select * from project_per_org_view
union all
(select o.orgid, null as projects_done, null as first_project,
max(sign_up_date), max(sign_up_date), name from org_sub o
where o.orgid not in (select orgid from project_per_org_view)
group by o.orgid, o.name);


CREATE OR REPLACE VIEW "project_per_org_view" AS
select p.orgid, count(distinct p.pid) as projects_done, min(p.created) as first_project,
max(o.sign_up_date) as signed_up,min(o.sign_up_date) as signed_up_2, o.name
from org_sub o join project_view p
on o.orgid = p.orgid
group by p.orgid, o.name
order by 3 asc;


CREATE OR REPLACE VIEW "subscription_plan" AS
select orgId, subscriptionplanid, count(subscriptionplanid) as countSubscriptionPlan from app_db_p.subscription_payment_history
group by orgId, subscriptionplanid order by countSubscriptionPlan desc



CREATE OR REPLACE VIEW churn_table AS
SELECT
  b.*
, (CASE WHEN ("week_spent" > 2) THEN 'not churn' ELSE 'churn' END) "churn_status"
FROM
  (
   SELECT
     a.*
   , "date_diff"('week', "a"."trial_date", "a"."last_date") "week_spent"
   FROM
     visualization_db.org_status a
)  b

-- Churn table
CREATE TABLE parquet_churn_table WITH (
          format = 'PARQUET',
          parquet_compression = 'SNAPPY',
          external_location = 's3://seamfix-machine-learning-ir/BioregistraParquet/churntable'
    ) AS SELECT * FROM visualization_db.churn_table


-- Org Sub
CREATE TABLE parquet_org_sub WITH (
          format = 'PARQUET',
          parquet_compression = 'SNAPPY',
          external_location = 's3://seamfix-machine-learning-ir/BioregistraParquet/orgSub'
    ) AS SELECT * FROM visualization_db.org_sub

-- Org status
CREATE TABLE parquet_org_status WITH (
          format = 'PARQUET',
          parquet_compression = 'SNAPPY',
          external_location = 's3://seamfix-machine-learning-ir/BioregistraParquet/orgStatus'
    ) AS SELECT * FROM visualization_db.org_status

-- Project Date
CREATE TABLE parquet_project_date WITH (
          format = 'PARQUET',
          parquet_compression = 'SNAPPY',
          external_location = 's3://seamfix-machine-learning-ir/BioregistraParquet/projectDate'
    ) AS SELECT * FROM visualization_db.project_date

-- Project Per OrgView
CREATE TABLE parquet_project_per_org_view WITH (
          format = 'PARQUET',
          parquet_compression = 'SNAPPY',
          external_location = 's3://seamfix-machine-learning-ir/BioregistraParquet/projectOrgView'
    ) AS SELECT * FROM visualization_db.project_per_org_view


 -- Project Per OrgView
CREATE TABLE parquet_project_view WITH (
          format = 'PARQUET',
          parquet_compression = 'SNAPPY',
          external_location = 's3://seamfix-machine-learning-ir/BioregistraParquet/projectView'
    ) AS SELECT * FROM visualization_db.project_view

 -- Subscription Plan
CREATE TABLE parquet_subscription_plan WITH (
          format = 'PARQUET',
          parquet_compression = 'SNAPPY',
          external_location = 's3://seamfix-machine-learning-ir/BioregistraParquet/subscriptionPlan'
    ) AS SELECT * FROM visualization_db.subscription_plan



    --- Updates on Visuaization
    -- Note: Unless you save your query, these tabs will NOT persist if you clear your cookies or change browsers.

SELECT * FROM app_db_p.sign_up_parquet;


SELECT * FROM app_db_p.sign_up_parquet
WHERE customer_feedback IS NOT NULL


--Total feedbacks received

SELECT DISTINCT reason_for_signup FROM app_db_p.sign_up_parquet

SELECT DISTINCT refered_by FROM app_db_p.sign_up_parquet

SELECT DISTINCT how_did_u_hear_abt_us FROM app_db_p.sign_up_parquet

SELECT DISTINCT which_person FROM app_db_p.sign_up_parquet

SELECT next_followup_date FROM app_db_p.sign_up_parquet HAVING next_followup_date IS NOT NULL

SELECT DISTINCT prefered_form_type FROM app_db_p.sign_up_parquet

SELECT DISTINCT call_comments FROM app_db_p.sign_up_parquet


-- Query for total number of forms

SELECT SUM(num_forms) FROM app_db_p.sign_up_parquet

SELECT SUM(number_of_published_forms) FROM app_db_p.sign_up_parquet

-- Query for total number of forms number_of_published_forms

SELECT * FROM app_db_p.sign_up_parquet

---Sign up templates that converted more

SELECT sign_up_template, COUNT(*) as use_sign_up_template_count
FROM app_db_p.sign_up_parquet GROUP BY sign_up_template


SELECT sign_up_template, template_name, COUNT(*) as sign_up_template_count, SUM(num_forms) as total_forms,
SUM(number_of_published_forms) as total_published_forms
FROM app_db_p.sign_up_parquet GROUP BY sign_up_template, template_name
