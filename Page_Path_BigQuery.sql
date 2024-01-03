select
    case when split(split((select value.string_value from unnest(event_params) where event_name = 'page_view' and key = 'page_location'),'/')[safe_ordinal(4)],'?')[safe_ordinal(1)] = '' then null else concat('/',split(split((select value.string_value from unnest(event_params) where event_name = 'page_view' and key = 'page_location'),'/')[safe_ordinal(4)],'?')[safe_ordinal(1)]) end as pagepath_level_1,
    case when split(split((select value.string_value from unnest(event_params) where event_name = 'page_view' and key = 'page_location'),'/')[safe_ordinal(5)],'?')[safe_ordinal(1)] = '' then null else concat('/',split(split((select value.string_value from unnest(event_params) where event_name = 'page_view' and key = 'page_location'),'/')[safe_ordinal(5)],'?')[safe_ordinal(1)]) end as pagepath_level_2,
    case when split(split((select value.string_value from unnest(event_params) where event_name = 'page_view' and key = 'page_location'),'/')[safe_ordinal(6)],'?')[safe_ordinal(1)] = '' then null else concat('/',split(split((select value.string_value from unnest(event_params) where event_name = 'page_view' and key = 'page_location'),'/')[safe_ordinal(6)],'?')[safe_ordinal(1)]) end as pagepath_level_3,
        countif(event_name = 'page_view') as page_views,
  device.web_info.hostname,
  (select value.string_value from unnest(event_params) where event_name = 'page_view' and key = 'page_location') as page
from
    -- change this to your google analytics 4 export location in bigquery
    ``
where
    -- define static and/or dynamic start and end date
    _table_suffix between '20220901'
    and format_date('%Y%m%d',date_sub(current_date(), interval 1 day))
group by
    pagepath_level_1,
    pagepath_level_2,
    pagepath_level_3,
    hostname,
    page
order by
    page_views desc
