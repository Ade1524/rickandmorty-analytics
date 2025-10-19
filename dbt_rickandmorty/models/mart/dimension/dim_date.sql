-- using a recursive cte to create a date dimension
-- one row per every day
with recursive params(start_date, end_date, day_interval) as (
  select '2017-01-01'::date, '2025-12-31'::date, 1
)
  
, date_spine as (
  select start_date as date_day
    from params

    union all

  select date_day + day_interval
    from date_spine
        ,params
   where date_day < end_date
)
  
, unknown_record as (
  select -1 as unknown_key
        ,'Unknown' as unknown_text
        ,0 as unknown_integer
        ,0.0 as unknown_float
        ,false as unknown_boolean
        ,null as unknown_date
        ,null as unknown_null
)
, calendar as (
  select to_char(date_day, 'YYYYMMDD') as dim_date_key
        ,date_day::date as date_day
        ,to_char(date_day, 'YYYY-MM-DD') as date_iso
        ,to_char(date_day, 'DD/MM/YYYY') as date_gb
        ,to_char(date_day, 'MM/DD/YYYY') as date_us
        ,to_char(date_day, 'DAY') as day_of_week
        ,dayofweekiso(date_day) as day_of_week_number
        ,to_char(date_day, 'DY') as day_of_week_abbr
        ,to_char(date_day, 'DD') as day_of_month
        ,day(date_day) as day_of_month_number
        ,dayofyear(date_day) as day_of_year
        ,weekofyear(date_day) as week_of_year
        ,to_char(date_day, 'MM') as month_of_year
        ,to_char(date_day, 'MMMM') as month_name
        ,to_char(date_day, 'MON') as month_name_abbr
        ,date_trunc('MONTH', date_day)::date as first_day_of_month
        ,last_day(date_day)::date as last_day_of_month
        ,'Q'||quarter(date_day) as quarter_of_year
        ,to_char(date_day, 'YYYY') as year
        ,'C'||to_char(date_day, 'YY') as year_abbr
        ,datediff('day', current_date(), date_day) as day_offset
        ,datediff('week', current_date(), date_day) as week_offset
        ,datediff('month', current_date(), date_day) as month_offset
        ,datediff('quarter', current_date(), date_day) as quarter_offset
        ,datediff('year', current_date(), date_day) as year_offset
        ,dateadd('day', -7, date_day) as same_day_last_week
        ,dateadd('day', -14, date_day) as same_day_last_fortnight
        ,dateadd('month', -1, date_day) as same_day_last_month
        ,dateadd('year', -1, date_day) as same_day_last_year
        ,dateadd('day', 7, date_day) as same_day_next_week
        ,dateadd('day', 14, date_day) as same_day_next_fortnight
        ,dateadd('month', 1, date_day) as same_day_next_month
        ,dateadd('year', 1, date_day) as same_day_next_year
        ,'Q'||quarter(dateadd('month', 6, date_day)) as fiscal_quarter_of_year
        ,year(dateadd('month', 6, date_day)) as fiscal_year
        ,'F'||to_char(dateadd('month', 6, date_day), 'YY') as fiscal_year_abbr
    from date_spine
)

, final as (
    
    select unknown_key as dim_date_key
          ,unknown_null::date as date_day
          ,unknown_text as date_iso
          ,unknown_text as date_gb
          ,unknown_text as date_us
          ,unknown_integer as day_of_week_number
          ,unknown_text as day_of_week
          ,unknown_text as day_of_week_abbr
          ,unknown_text as day_of_month
          ,unknown_integer as day_of_month_number
          ,unknown_text as day_ordinal_indicator
          ,unknown_integer as day_of_year
          ,unknown_integer as week_of_year
          ,unknown_integer as month_of_year
          ,unknown_text as month_name
          ,unknown_text as month_name_abbr
          ,unknown_text as month_numbered
          ,unknown_date::date as first_day_of_month
          ,unknown_date::date as last_day_of_month
          ,unknown_text as quarter
          ,unknown_text as quarter_of_year
          ,unknown_integer as year
          ,unknown_text as year_abbr
          ,unknown_null as day_offset
          ,unknown_null as week_offset
          ,unknown_null as month_offset
          ,unknown_null as quarter_offset
          ,unknown_null as year_offset
          ,unknown_date::date as same_day_last_week
          ,unknown_date::date as same_day_last_fortnight
          ,unknown_date::date as same_day_last_month
          ,unknown_date::date as same_day_last_year
          ,unknown_date::date as same_day_next_week
          ,unknown_date::date as same_day_next_fortnight
          ,unknown_date::date as same_day_next_month
          ,unknown_date::date as same_day_next_year
          ,unknown_text as fiscal_quarter
          ,unknown_text as fiscal_quarter_of_year
          ,unknown_integer as fiscal_year
          ,unknown_text as fiscal_year_abbr
      from unknown_record

    union all
  
    select dim_date_key::int
          ,date_day
          ,date_iso
          ,date_gb
          ,date_us
          ,day_of_week_number
          ,trim(day_of_week) as day_of_week
          ,day_of_week_abbr
          ,day_of_month
          ,day_of_month_number
          ,case 
              when day_of_month_number in (1, 21, 31) then 'st'
              when day_of_month_number in (2, 22) then 'nd'
              when day_of_month_number in (3, 23) then 'rd'
              else 'th'
           end as day_ordinal_indicator
          ,day_of_year
          ,week_of_year
          ,month_of_year
          ,month_name
          ,month_name_abbr
          ,month_of_year||' '||month_name_abbr as month_numbered
          ,first_day_of_month
          ,last_day_of_month
          ,quarter_of_year||' '||year_abbr as quarter
          ,quarter_of_year
          ,year
          ,year_abbr
          ,day_offset
          ,week_offset
          ,month_offset
          ,quarter_offset
          ,year_offset
          ,same_day_last_week
          ,same_day_last_fortnight
          ,same_day_last_month
          ,same_day_last_year
          ,same_day_next_week
          ,same_day_next_fortnight
          ,same_day_next_month
          ,same_day_next_year
          ,fiscal_quarter_of_year||' '||fiscal_year_abbr as fiscal_quarter
          ,fiscal_quarter_of_year
          ,fiscal_year
          ,fiscal_year_abbr
      from calendar
)

select 
    {{ dbt_utils.generate_surrogate_key([ 'date_day' ]) }} as dim_date_sk,
    *
from final