with t as (
select approx_percentile(count_, 0.25) as first_quartile from (
select cardinality(regexp_extract_all(text, 'دلار')) as count_ from kafka."default".news_statistics)
)
select week, count(*) as count_, 'دلار' from(
select format_datetime("@timestamp", 'w') as week from 
(
select "@timestamp", cardinality(regexp_extract_all(text, 'دلار')) as count_, first_quartile as quartile
from kafka."default".news_statistics join t on true
)
where count_ > quartile
)
group by week;


with t as (
select approx_percentile(count_, 0.25) as first_quartile from (
select cardinality(regexp_extract_all(text, 'ارز')) as count_ from kafka."default".news_statistics)
)
select week, count(*) as count_, 'ارز' from(
select format_datetime("@timestamp", 'w') as week from 
(
select "@timestamp", cardinality(regexp_extract_all(text, 'ارز')) as count_, first_quartile as quartile
from kafka."default".news_statistics join t on true
)
where count_ > quartile
)
group by week;


with t as (
select approx_percentile(count_, 0.25) as first_quartile from (
select cardinality(regexp_extract_all(text, 'بیت کوین')) as count_ from kafka."default".news_statistics)
)
select week, count(*) as count_, 'بیت کوین' from(
select format_datetime("@timestamp", 'w') as week from 
(
select "@timestamp", cardinality(regexp_extract_all(text, 'بیت کوین')) as count_, first_quartile as quartile
from kafka."default".news_statistics join t on true
)
where count_ > quartile
)
group by week;


with t as (
select approx_percentile(count_, 0.25) as first_quartile from (
select cardinality(regexp_extract_all(text, 'ارز دیجیتال')) as count_ from kafka."default".news_statistics)
)
select week, count(*) as count_, 'ارز دیجیتال' from(
select format_datetime("@timestamp", 'w') as week from 
(
select "@timestamp", cardinality(regexp_extract_all(text, 'ارز دیجیتال')) as count_, first_quartile as quartile
from kafka."default".news_statistics join t on true
)
where count_ > quartile
)
group by week;

with t as (
select approx_percentile(count_, 0.25) as first_quartile from (
select cardinality(regexp_extract_all(text, 'طلا')) as count_ from kafka."default".news_statistics)
)
select week, count(*) as count_, 'طلا' from(
select format_datetime("@timestamp", 'w') as week from 
(
select "@timestamp", cardinality(regexp_extract_all(text, 'طلا')) as count_, first_quartile as quartile
from kafka."default".news_statistics join t on true
)
where count_ > quartile
)
group by week;






with t as (
select approx_percentile(count_, 0.25) as first_quartile from (
select cardinality(regexp_extract_all(text, 'دلار')) as count_ from kafka."default".news_statistics)
)
select day_, count(*) as count_, 'دلار' from(
select format_datetime("@timestamp", 'D') as day_ from 
(
select "@timestamp", cardinality(regexp_extract_all(text, 'دلار')) as count_, first_quartile as quartile
from kafka."default".news_statistics join t on true
)
where count_ > quartile
)
group by day_;


with t as (
select approx_percentile(count_, 0.25) as first_quartile from (
select cardinality(regexp_extract_all(text, 'ارز')) as count_ from kafka."default".news_statistics)
)
select day_, count(*) as count_, 'ارز' from(
select format_datetime("@timestamp", 'D') as day_ from 
(
select "@timestamp", cardinality(regexp_extract_all(text, 'ارز')) as count_, first_quartile as quartile
from kafka."default".news_statistics join t on true
)
where count_ > quartile
)
group by day_;


with t as (
select approx_percentile(count_, 0.25) as first_quartile from (
select cardinality(regexp_extract_all(text, 'بیت کوین')) as count_ from kafka."default".news_statistics)
)
select day_, count(*) as count_, 'بیت کوین' from(
select format_datetime("@timestamp", 'D') as day_ from 
(
select "@timestamp", cardinality(regexp_extract_all(text, 'بیت کوین')) as count_, first_quartile as quartile
from kafka."default".news_statistics join t on true
)
where count_ > quartile
)
group by day_;


with t as (
select approx_percentile(count_, 0.25) as first_quartile from (
select cardinality(regexp_extract_all(text, 'ارز دیجیتال')) as count_ from kafka."default".news_statistics)
)
select day_, count(*) as count_, 'ارز دیجیتال' from(
select format_datetime("@timestamp", 'D') as day_ from 
(
select "@timestamp", cardinality(regexp_extract_all(text, 'ارز دیجیتال')) as count_, first_quartile as quartile
from kafka."default".news_statistics join t on true
)
where count_ > quartile
)
group by day_;

with t as (
select approx_percentile(count_, 0.25) as first_quartile from (
select cardinality(regexp_extract_all(text, 'طلا')) as count_ from kafka."default".news_statistics)
)
select day_, count(*) as count_, 'طلا' from(
select format_datetime("@timestamp", 'D') as day_ from 
(
select "@timestamp", cardinality(regexp_extract_all(text, 'طلا')) as count_, first_quartile as quartile
from kafka."default".news_statistics join t on true
)
where count_ > quartile
)
group by day_;



