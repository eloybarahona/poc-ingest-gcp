''' 
Number of employees hired for each job and department in 2021 divided by quarter. The
table must be ordered alphabetically by department and job.
'''
with fin as (
	select 
		d.department as department,
		j.job as job, 
		a.`datetime` as date_origin, 
		STR_TO_DATE(a.`datetime`,'%Y-%m-%d') as date_new,
		case 
			when STR_TO_DATE(a.`datetime`,'%Y-%m-%d') >= '2021-01-01' and STR_TO_DATE(a.`datetime`,'%Y-%m-%d') < '2021-04-01' then 'Q1'
			when STR_TO_DATE(a.`datetime`,'%Y-%m-%d') >= '2021-04-01' and STR_TO_DATE(a.`datetime`,'%Y-%m-%d') < '2021-07-01' then 'Q2'
			when STR_TO_DATE(a.`datetime`,'%Y-%m-%d') >= '2021-07-01' and STR_TO_DATE(a.`datetime`,'%Y-%m-%d') < '2021-10-01' then 'Q3'
			when STR_TO_DATE(a.`datetime`,'%Y-%m-%d') >= '2021-10-01' and STR_TO_DATE(a.`datetime`,'%Y-%m-%d') < '2022-01-01' then 'Q4'
			else 'NO'
		end q,		
		count(1) 
	from hired_employees a 
		left join departments d on (a.departament_id = d.id)
		left join jobs j on (a.job_id = j.id)
	where year(STR_TO_DATE(a.`datetime`,'%Y-%m-%d')) = '2021'
	group by d.department, j.job, a.`datetime`
)
select
	department,
	job,
	SUM(CASE WHEN (q='Q1') THEN 1 ELSE 0 END) as 'Q1',
	SUM(CASE WHEN (q='Q2') THEN 1 ELSE 0 END) as 'Q2',
	SUM(CASE WHEN (q='Q3') THEN 1 ELSE 0 END) as 'Q3',
	SUM(CASE WHEN (q='Q4') THEN 1 ELSE 0 END) as 'Q4'
from
fin
group by
	department, job
order by department, job

''' 
List of ids, name and number of employees hired of each department that hired more
employees than the mean of employees hired in 2021 for all the departments, ordered
by the number of employees hired (descending).
'''
with media as (
	select 
		(count(a.id) / count(distinct d.id)) as dato
	from hired_employees a 
		left join departments d on (a.departament_id = d.id)
	where year(STR_TO_DATE(a.`datetime`,'%Y-%m-%d')) = '2021'
), fin as (
	select 
		d.id as id,
		d.department as department,
		count(1) as cantidad
	from hired_employees a 
		left join departments d on (a.departament_id = d.id)
	group by d.id, d.department
)
select 
	id, 
	department, 
	cantidad
from fin
where cantidad > (select dato from media)
order by cantidad desc
