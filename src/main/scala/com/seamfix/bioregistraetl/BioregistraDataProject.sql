SELECT *
FROM  br.project_update;

SELECT *
FROM  br.project_update where form_type is NULL;


SELECT ba."Subcription Date" as subscription_date,
ba."number of forms" as num_forms,
ba.form_name,
ba.customer_email,
ba.number_of_published_forms,
ba.country
FROM  br.analysis ba;


select count(*) from br.analysis ba;