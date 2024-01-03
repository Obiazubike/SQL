SELECT
  IFNULL(
    CONCAT(session_source, ' / ', session_medium),
    CONCAT(first_user_source, ' / ', first_user_medium)
  ) AS session_source_medium,
  IFNULL(session_medium, first_user_medium) AS session_medium,
  IFNULL(session_campaign_name, first_user_campaign_name) AS session_campaign_name,
  landing_page,
  landing_page_title,
  host_name,
  region,
  country,
  city,
  DATE,
  event_name,
  LANGUAGE,
  COUNT(DISTINCT session_id) AS sessions,
  SUM(CAST(engaged_sessions AS INT)) AS engaged_sessions,
  COUNT(DISTINCT new_users) AS new_users,
  SUM(event_count) AS event_count,
  COUNT(DISTINCT user_pseudo_id) AS total_users,
  SAFE_DIVIDE(
    COUNT(DISTINCT session_id),
    COUNT(DISTINCT user_pseudo_id)
  ) AS sessions_per_user,
  SAFE_DIVIDE(
    SUM(CAST(engaged_sessions AS INT)),
    COUNT(DISTINCT session_id)
  ) AS engagement_rate
FROM
  (
    SELECT
      MAX(
        CASE
          WHEN (
            SELECT
              value.int_value
            FROM
              UNNEST (event_params)
            WHERE
              event_name = 'page_view'
              AND KEY = 'entrances'
          ) = 1
          AND (
            SELECT
              value.string_value
            FROM
              UNNEST (event_params)
            WHERE
              event_name = 'page_view'
              AND KEY = 'source'
          ) IS NULL THEN CONCAT(traffic_source.source)
          ELSE (
            SELECT
              value.string_value
            FROM
              UNNEST (event_params)
            WHERE
              KEY = 'source'
          )
        END
      ) AS session_source,
      traffic_source.source AS first_user_source,
      MAX(
        CASE
          WHEN (
            SELECT
              value.int_value
            FROM
              UNNEST (event_params)
            WHERE
              event_name = 'page_view'
              AND KEY = 'entrances'
          ) = 1
          AND (
            SELECT
              value.string_value
            FROM
              UNNEST (event_params)
            WHERE
              event_name = 'page_view'
              AND KEY = 'source'
          ) IS NULL THEN CONCAT(traffic_source.medium)
          ELSE (
            SELECT
              value.string_value
            FROM
              UNNEST (event_params)
            WHERE
              KEY = 'medium'
          )
        END
      ) AS session_medium,
      traffic_source.medium AS first_user_medium,
      MAX(
        CASE
          WHEN (
            SELECT
              value.int_value
            FROM
              UNNEST (event_params)
            WHERE
              event_name = 'page_view'
              AND KEY = 'entrances'
          ) = 1
          AND (
            SELECT
              value.string_value
            FROM
              UNNEST (event_params)
            WHERE
              event_name = 'page_view'
              AND KEY = 'source'
          ) IS NULL THEN CONCAT(traffic_source.name)
          ELSE (
            SELECT
              value.string_value
            FROM
              UNNEST (event_params)
            WHERE
              KEY = 'campaign'
          )
        END
      ) AS session_campaign_name,
      traffic_source.name AS first_user_campaign_name,
      MAX(
        (
          SELECT
            value.string_value
          FROM
            UNNEST (event_params)
          WHERE
            event_name = 'session_start'
            AND key = 'page_location'
        )
      ) AS landing_page,
      MAX(
        (
          SELECT
            value.string_value
          FROM
            UNNEST (event_params)
          WHERE
            event_name = 'session_start'
            AND key = 'page_title'
        )
      ) AS landing_page_title,
      device.web_info.hostname AS host_name,
      geo.region AS region,
      geo.country AS country,
      geo.city AS city,
      event_date AS DATE,
      event_name AS event_name,
      device.language AS LANGUAGE,
      CONCAT(
        user_pseudo_id,
        (
          SELECT
            value.int_value
          FROM
            UNNEST (event_params)
          WHERE
            key = 'ga_session_id'
        )
      ) AS session_id,
      MAX(
        (
          SELECT
            value.string_value
          FROM
            UNNEST (event_params)
          WHERE
            key = 'session_engaged'
        )
      ) AS engaged_sessions,
      MAX(
        CASE
          WHEN event_name = 'first_visit' THEN user_pseudo_id
          ELSE NULL
        END
      ) AS new_users,
      user_pseudo_id,
      COUNT(event_name) AS event_count
    FROM
      `nutrien-ekonomics.analytics_310505747.events_*`
    WHERE
      _TABLE_SUFFIX BETWEEN FORMAT_DATE('%Y%m%d', DATE_TRUNC(CURRENT_DATE(), MONTH)) AND FORMAT_DATE(
        '%Y%m%d',
        DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
      )
    GROUP BY
      first_user_source,
      first_user_medium,
      first_user_campaign_name,
      host_name,
      region,
      country,
      city,
      DATE,
      event_name,
      LANGUAGE,
      session_id,
      user_pseudo_id
  )
GROUP BY
  session_source_medium,
  session_medium,
  session_campaign_name,
  landing_page,
  landing_page_title,
  host_name,
  region,
  country,
  city,
  DATE,
  event_name,
  LANGUAGE
