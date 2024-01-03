WITH

source AS (
    SELECT * FROM `[PROJECT]`.`[DATASET]`.`events_*`
),

user_journey AS (
    SELECT
        user_pseudo_id AS uid,

        (SELECT value.string_value FROM
        UNNEST(event_params)
        WHERE
        key = 'page_location') AS page,

        (SELECT value.string_value FROM
        UNNEST(event_params)
        WHERE
        key = 'page_title') AS page_title,

        (SELECT value.int_value FROM
        UNNEST(event_params)
        WHERE
        key = 'ga_session_id') AS session_id,

        (SELECT value.int_value FROM
        UNNEST(event_params)
        WHERE
        key = 'ga_session_number') AS session_number,

        (SELECT value.string_value FROM
        UNNEST(event_params)
        WHERE
        key = 'medium') AS medium,

        (SELECT value.string_value FROM
        UNNEST(event_params)
        WHERE
        key = 'source') AS source,

        CAST(event_timestamp AS NUMERIC) as numericTimestamp,
        event_name AS event_type,
        device.category AS device_category,
        device.mobile_brand_name AS device_brand_name,
        TIMESTAMP_MICROS(user_first_touch_timestamp) AS visitor_first_touch_at,
        geo.country,
    FROM source
)

SELECT
   *
FROM user_journey
where user_journey.event_type = 'page_view'
