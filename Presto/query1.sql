select category, count(*) as count_ from kafka."default".news_statistics where 
"@timestamp" between timestamp '2022-06-23 00:00' and timestamp '2022-06-23 23:00' 
group by category order by count_ desc ;


