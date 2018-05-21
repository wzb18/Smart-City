--check grid data
select city_code, city_name, min(ext_min_x) as min_x, min(ext_min_y) as min_y, avg(ext_max_x-ext_min_x) as dis_lon, avg(ext_max_y-ext_min_y) as dis_lat 
from ss_grid_wgs84 group by city_code, city_name;

--创建配置表， 用于匹配小时时间跨度配置表temp_lc_dathour
create table temp_lc_dathour as 
select a.stimeh as beginh, a.etimeh as endh, b.matchh from( 
select substr(stime, 12, 2) as stimeh, substr(etime, 12, 2) as etimeh, 'aa' as index_join from openlab.stay_month where date = 20170901 and city = 'V0110000' 
group by substr(stime, 12, 2), substr(etime, 12, 2)
)a left outer join (
select substr(stime, 12, 2) as matchh, 'aa' as index_join from stay_month where date = 20170901 and city = 'V0110000' group by substr(stime, 12, 2) 
)b on (a.index_join = b.index_join)
where a.etimeh >= b.matchh and a.stimeh <= b.matchh order by beginh, endh, matchh;

-- clean raw data
create table temp_stay_poi_new_20180416 as
select a.uid, a.city, a.date, ptype,poi_id, gender, age, is_core, weight, a.weekday_day_time as secs, 
concat(ceil((weighted_centroid_lat - ss.min_y )/ ss.dis_lat), '-', ceil((weighted_centroid_lon - ss.min_x )/ ss.dis_lon )) as grid_find 
from stay_poi a 
inner join (select uid, city, date, gender, age, gw as weight from openlab.user_attribute where city = 'V0110000' and date = 20170901)u on(a.uid = u.uid and a.city = u.city and a.date = u.date)
inner join (
select city_code, min(ext_min_x) as min_x, min(ext_min_y) as min_y, avg(ext_max_x-ext_min_x) as dis_lon, avg(ext_max_y-ext_min_y) as dis_lat 
from ss_grid_wgs84 group by city_code
)ss on (a.city = ss.city_code)
where a.city in ('V0110000') and a.date in (20170901);


create table temp_stay_month_new_20180416 as 
select c.city, c.month, c.uid, c.poi_id, c.ptype, c.date, stime, etime, gender, age, p.is_core, weight, 
(unix_timestamp(etime,'yyyy-MM-dd hh:mm:ss') - unix_timestamp(stime,'yyyy-MM-dd hh:mm:ss')) as duration, grid_find  
from(
select a.uid, a.poi_id, a.ptype, a.city, a.date, round(a.date, -2) + 1 as month,
case when stime <> stime_first then stime else cast(date_format(stime, 'yyyy-MM-dd 00:00:00') as timestamp) end as stime, 
case when etime <> etime_last then etime else cast(date_format(etime, 'yyyy-MM-dd 23:59:59') as timestamp) end as etime
from stay_month a 
inner join (
select uid, city, date, min(stime) as stime_first, max(etime) as etime_last
from stay_month where cast(date/100 as int) in (201709) and city in ('V0110000')
group by uid, city, date
)b on(a.uid = b.uid and a.city = b.city and a.date = b.date)
where cast(a.date/100 as int) in (201709) and a.city in ('V0110000') and (unix_timestamp(etime,'yyyy-MM-dd hh:mm:ss') - unix_timestamp(stime,'yyyy-MM-dd hh:mm:ss')) > 0
)c 
inner join temp_stay_poi_new_20180416 p on(c.uid = p.uid and c.city = p.city and c.month = p.date and c.ptype = p.ptype and c.poi_id = p.poi_id)
where (unix_timestamp(etime,'yyyy-MM-dd hh:mm:ss') - unix_timestamp(stime,'yyyy-MM-dd hh:mm:ss')) >= 1800;



drop table temp_work_tmp_20180416;
create table temp_work_tmp_20180416 as 
select uid, city, date, ptype, grid_find, secs, filter_index from(
select uid, city, date, ptype, secs, row_number() over(partition by city, date, uid order by secs desc) as filter_index, grid_find from temp_stay_poi_new_20180416
)a where filter_index <= 2 and not exists (select 1 from temp_stay_poi_new_20180416 c where c.uid=a.uid and c.ptype = 2 and c.city = a.city and c.date = a.date);



CREATE TABLE temp_work_use_20180416 AS 
SELECT uid, city, date, workid FROM ( 
SELECT uid, city, date , CASE  WHEN ptype = 1 AND secs2 >= secs1 * 0.6 THEN grid2 ELSE grid1 END AS workid 
FROM ( 
SELECT a.uid, a.city, a.date, a.ptype, a.grid_find AS grid1 , a.secs AS secs1, b.grid_find AS grid2, b.secs AS secs2 
FROM ( SELECT uid, city, date, ptype, grid_find , secs, filter_index FROM temp_work_tmp_20180416 WHERE filter_index = 1 )a 
LEFT JOIN ( SELECT uid, city, date, ptype, grid_find , secs, filter_index FROM temp_work_tmp_20180416 WHERE filter_index = 2 )b 
ON (a.uid = b.uid AND a.city = b.city AND a.date = b.date) ) a 
UNION ALL SELECT uid, city, date, grid_find AS workid FROM temp_stay_poi_new_20180416 WHERE ptype = 2 ) w;



create table temp_comsume_use_20180416 as 
select uid, city, month, consumid from(
select uid, city, month, grid_find as consumid, row_number() over (partition by uid, city, month order by duration desc) as filter_index 
from(
select sm.uid, sm.city, sm.month, sm.grid_find,duration 
from temp_stay_month_new_20180416 sm 
left outer join (select uid, city, date, workid, 'aa' as select_index from temp_work_use_20180416) b
on (sm.uid = b.uid and sm.city = b.city and sm.month = b.date and sm.grid_find = b.workid)
where duration/3600 >=2 and datediff(stime, '2017-08-26') % 7 <= 1
and hour(sm.stime) >= 10 and hour(sm.etime) < 20 and ptype = 0 and b.select_index is null
)a 
)a
where filter_index = 1
;

--- home, work, job grid id 
create table temp_home_job_consum_out_20180416 as
select a.city, a.month, case when homeid is null then -1 else homeid end as homeid, 
case when workid is null then -1 else workid end as workid, 
case when consumid is null then -1 else consumid end as consumid, 
case when gender = '01' then 'M' 
      when gender = '02' then 'F' 
      when gender = '03' then '-1' 
      end as gender_level,
case when age = '01' then '00-06' 
      when age = '02' then '07-12' 
      when age = '03' then '13-15' 
      when age = '04' then '16-18' 
      when age = '05' then '19-24' 
      when age = '06' then '25-29' 
      when age = '07' then '30-34' 
      when age = '08' then '35-39' 
      when age = '09' then '40-44' 
      when age = '10' then '45-49' 
      when age = '11' then '50-54' 
      when age = '12' then '55-59'
      when age = '13' then '60-64'
      when age = '14' then '65-69' 
      when age = '15' then '70以上'
      when age = '16' then '-1'
      end as age_level, is_core,
count(1) as pop_num, cast(round(sum(a.weight)) as bigint) as pop_wnum,
cast(row_number() over(partition by city, month) / 200000 as int) + 1 as slides
from(
select city, month, uid, gender, age, is_core, weight from temp_stay_month_new_20180416 group by city, month, uid, gender, age, is_core, weight
)a left outer join(
select city, month, uid, grid_find as homeid
from temp_stay_month_new_20180416 where ptype = 1 
group by city, month, uid, grid_find
)b on(a.uid = b.uid and a.city = b.city and a.month = b.month)
left outer join temp_work_use_20180416 c on(a.uid = c.uid and a.city = c.city and a.month = c.date)
left outer join temp_comsume_use_20180416 d on(a.uid = d.uid and a.city = d.city and a.month = d.month)
group by a.city, a.month, case when homeid is null then -1 else homeid end, 
case when workid is null then -1 else workid end, 
case when consumid is null then -1 else consumid end, is_core,
case when gender = '01' then 'M' 
      when gender = '02' then 'F' 
      when gender = '03' then '-1' 
      end,
case when age = '01' then '00-06' 
      when age = '02' then '07-12' 
      when age = '03' then '13-15' 
      when age = '04' then '16-18' 
      when age = '05' then '19-24' 
      when age = '06' then '25-29' 
      when age = '07' then '30-34' 
      when age = '08' then '35-39' 
      when age = '09' then '40-44' 
      when age = '10' then '45-49' 
      when age = '11' then '50-54' 
      when age = '12' then '55-59'
      when age = '13' then '60-64'
      when age = '14' then '65-69' 
      when age = '15' then '70以上'
      when age = '16' then '-1'
      end;
      
      
---every day hour statistics  
create table temp_lc_hour_pop_num_out_20180416 as
select city, date, matchh, grid_find, sum(num_pop) as pop_num, cast(round(sum(wnum_pop)) as bigint) as pop_wnum,
cast(row_number() over(partition by city, date) / 400000 as int) + 1 as slides
from(
select s1.date, s1.city, s1.grid_find, s1.num_pop, s1.wnum_pop, c.matchh 
from(
select a.date, a.city, substr(stime, 12, 2) as stimeh, substr(etime, 12, 2) as etimeh, grid_find,
count(1) as num_pop, sum(weight) as wnum_pop  
from temp_stay_month_new_20180416 a 
group by a.date, a.city, substr(stime, 12, 2), substr(etime, 12, 2), grid_find
)s1 
left outer join temp_lc_dathour c on (s1.stimeh = c.beginh and s1.etimeh = c.endh)
)a group by city, date, matchh, grid_find;


-- stay duration time
drop table temp_lc_grid_duration_out_20180416;
create table temp_lc_grid_duration_out_20180416 as
select a.city, a.month, a.ptype, is_core, grid_find, hourgap, 
count(1) as pop_num, cast(round(sum(weight)) as bigint) as pop_wnum,
cast(row_number() over(partition by a.city, a.month) / 250000 as int) + 1 as slides 
from(
select city, month, uid, weight, ptype, is_core, grid_find, floor(sum(duration) /3600/count(1)) as hourgap 
from
(select city, month, uid, weight, ptype, is_core, grid_find,date,case when sum(duration)>= 86400 then 86399 else sum(duration) end as duration
from temp_stay_month_new_20180416 where ptype = 1
group by city, month, uid, weight, ptype, is_core, grid_find,date)a
group by city, month, uid, weight, ptype, is_core, grid_find
)a group by a.city, a.month, a.ptype, is_core, grid_find, hourgap;


--calculate slides
select city, month, max(slides) as city_slides from temp_home_job_consum_out_20180416 group by city, month;

-- select data out 
select homeid, jobid, comsumeid,gender_level, age_level, is_core, pop_num, wpop_num, slides 
from temp_home_job_consum_out_20180416 where month = 20170901 and city = 'V0110000' and slides = 1;
