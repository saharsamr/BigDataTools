select week_day, (sum(count_) / count(*)) as avg from 
(
select count(*) as count_, week_day, year_, day_of_year from 
(
select format_datetime("@timestamp", 'E') as week_day, 
format_datetime("@timestamp", 'Y') as year_, 
format_datetime("@timestamp", 'D') as day_of_year
from kafka."default".news_statistics
) 
group by (week_day, year_, day_of_year)
) group by week_day order by avg desc;



select month_day, (sum(count_) / count(*)) as avg from 
(
select count(*) as count_, month_day, year_, day_of_year from 
(
select format_datetime("@timestamp", 'd') as month_day, 
format_datetime("@timestamp", 'Y') as year_, 
format_datetime("@timestamp", 'D') as day_of_year
from kafka."default".news_statistics
) 
group by (month_day, year_, day_of_year)
) group by month_day order by avg desc;


