select symbol, min(price_irr) as min_price, max(price_irr) as max_price, avg(price_irr) as mean_price 
from kafka."default".crypto_statistics 
where "@timestamp" between timestamp '2022-06-23 00:00' and timestamp '2022-06-23 23:00' 
group by symbol;