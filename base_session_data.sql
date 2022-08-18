-------------------------------------
-- All credit goes to the Looker dev team for the approach below.
-- This SQL can be run against the sessions_ table to extract a lot of common session-oriented metrics
-- This sql has the join of the unnested events data removed as to not duplicate counts of session-oriented data
-- This custom sql can go inside Data Studio or Tableau or... and provide a nice data schema of base elements for reporting.
-------------------------------------

SELECT
    sessions.partition_date AS session_date,
    sessions.session_date AS session_timestamp,
    sessions.user_pseudo_id  AS sessions_user_pseudo_id,
        (CASE WHEN ((TIMESTAMP_DIFF(sessions.session_data.session_end, sessions.session_data.session_start, second))/86400.0) = 0  THEN 'Yes' ELSE 'No' END) AS sessions_session_data_is_bounce,
    sessions.ga_session_id  AS sessions_ga_session_id,
        (DATE(sessions.session_data.session_start )) AS sessions_session_data_session_start_date,
        (DATE(sessions.session_data.session_end )) AS sessions_session_data_session_end_date,
    (CASE
WHEN (((TIMESTAMP_DIFF(sessions.session_data.session_end, sessions.session_data.session_start, second))/86400.0)*86400.0)  < 10 THEN '00'
WHEN (((TIMESTAMP_DIFF(sessions.session_data.session_end, sessions.session_data.session_start, second))/86400.0)*86400.0)  >= 10 AND (((TIMESTAMP_DIFF(sessions.session_data.session_end, sessions.session_data.session_start, second))/86400.0)*86400.0)  < 30 THEN '01'
WHEN (((TIMESTAMP_DIFF(sessions.session_data.session_end, sessions.session_data.session_start, second))/86400.0)*86400.0)  >= 30 AND (((TIMESTAMP_DIFF(sessions.session_data.session_end, sessions.session_data.session_start, second))/86400.0)*86400.0)  < 60 THEN '02'
WHEN (((TIMESTAMP_DIFF(sessions.session_data.session_end, sessions.session_data.session_start, second))/86400.0)*86400.0)  >= 60 AND (((TIMESTAMP_DIFF(sessions.session_data.session_end, sessions.session_data.session_start, second))/86400.0)*86400.0)  < 120 THEN '03'
WHEN (((TIMESTAMP_DIFF(sessions.session_data.session_end, sessions.session_data.session_start, second))/86400.0)*86400.0)  >= 120 AND (((TIMESTAMP_DIFF(sessions.session_data.session_end, sessions.session_data.session_start, second))/86400.0)*86400.0)  < 180 THEN '04'
WHEN (((TIMESTAMP_DIFF(sessions.session_data.session_end, sessions.session_data.session_start, second))/86400.0)*86400.0)  >= 180 AND (((TIMESTAMP_DIFF(sessions.session_data.session_end, sessions.session_data.session_start, second))/86400.0)*86400.0)  < 240 THEN '05'
WHEN (((TIMESTAMP_DIFF(sessions.session_data.session_end, sessions.session_data.session_start, second))/86400.0)*86400.0)  >= 240 AND (((TIMESTAMP_DIFF(sessions.session_data.session_end, sessions.session_data.session_start, second))/86400.0)*86400.0)  < 300 THEN '06'
WHEN (((TIMESTAMP_DIFF(sessions.session_data.session_end, sessions.session_data.session_start, second))/86400.0)*86400.0)  >= 300 AND (((TIMESTAMP_DIFF(sessions.session_data.session_end, sessions.session_data.session_start, second))/86400.0)*86400.0)  < 600 THEN '07'
WHEN (((TIMESTAMP_DIFF(sessions.session_data.session_end, sessions.session_data.session_start, second))/86400.0)*86400.0)  >= 600 THEN '08'
ELSE '09'
END) AS sessions_session_data_session_duration_tier__sort_,
    (CASE
WHEN (((TIMESTAMP_DIFF(sessions.session_data.session_end, sessions.session_data.session_start, second))/86400.0)*86400.0)  < 10 THEN 'Below 10'
WHEN (((TIMESTAMP_DIFF(sessions.session_data.session_end, sessions.session_data.session_start, second))/86400.0)*86400.0)  >= 10 AND (((TIMESTAMP_DIFF(sessions.session_data.session_end, sessions.session_data.session_start, second))/86400.0)*86400.0)  < 30 THEN '10 to 29'
WHEN (((TIMESTAMP_DIFF(sessions.session_data.session_end, sessions.session_data.session_start, second))/86400.0)*86400.0)  >= 30 AND (((TIMESTAMP_DIFF(sessions.session_data.session_end, sessions.session_data.session_start, second))/86400.0)*86400.0)  < 60 THEN '30 to 59'
WHEN (((TIMESTAMP_DIFF(sessions.session_data.session_end, sessions.session_data.session_start, second))/86400.0)*86400.0)  >= 60 AND (((TIMESTAMP_DIFF(sessions.session_data.session_end, sessions.session_data.session_start, second))/86400.0)*86400.0)  < 120 THEN '60 to 119'
WHEN (((TIMESTAMP_DIFF(sessions.session_data.session_end, sessions.session_data.session_start, second))/86400.0)*86400.0)  >= 120 AND (((TIMESTAMP_DIFF(sessions.session_data.session_end, sessions.session_data.session_start, second))/86400.0)*86400.0)  < 180 THEN '120 to 179'
WHEN (((TIMESTAMP_DIFF(sessions.session_data.session_end, sessions.session_data.session_start, second))/86400.0)*86400.0)  >= 180 AND (((TIMESTAMP_DIFF(sessions.session_data.session_end, sessions.session_data.session_start, second))/86400.0)*86400.0)  < 240 THEN '180 to 239'
WHEN (((TIMESTAMP_DIFF(sessions.session_data.session_end, sessions.session_data.session_start, second))/86400.0)*86400.0)  >= 240 AND (((TIMESTAMP_DIFF(sessions.session_data.session_end, sessions.session_data.session_start, second))/86400.0)*86400.0)  < 300 THEN '240 to 299'
WHEN (((TIMESTAMP_DIFF(sessions.session_data.session_end, sessions.session_data.session_start, second))/86400.0)*86400.0)  >= 300 AND (((TIMESTAMP_DIFF(sessions.session_data.session_end, sessions.session_data.session_start, second))/86400.0)*86400.0)  < 600 THEN '300 to 599'
WHEN (((TIMESTAMP_DIFF(sessions.session_data.session_end, sessions.session_data.session_start, second))/86400.0)*86400.0)  >= 600 THEN '600 or Above'
ELSE 'Undefined'
END) AS sessions_session_data_session_duration_tier,
    ((TIMESTAMP_DIFF(sessions.session_data.session_end, sessions.session_data.session_start, second))/86400.0)   AS sessions_session_data_session_duration,
    sessions.session_data.session_page_view_count  AS sessions_session_data_page_view_count,
        (CASE WHEN sessions.session_data.is_first_visit_session  THEN 'Yes' ELSE 'No' END) AS sessions_session_data_is_first_visit_session,
        (CASE WHEN sessions.session_data.is_engaged_session  THEN 'Yes' ELSE 'No' END) AS sessions_session_data_is_engaged_session,
    sessions.device_data.device__time_zone_offset_seconds  AS sessions_device_data_time_zone_offset_seconds,
    sessions.device_data.device__is_mobile  AS sessions_device_is_mobile,
    sessions.device_data.device__is_limited_ad_tracking  AS sessions_device_data_is_limited_ad_tracking,
    sessions.session_attribution.source||' '||sessions.session_attribution.medium  AS sessions_session_attribution_source_medium,
    sessions.session_attribution.source  AS sessions_session_attribution_source,
    sessions.session_attribution.medium  AS sessions_session_attribution_medium,
    case when sessions.session_attribution.source = '(direct)'
                 and (sessions.session_attribution.medium = '(none)' or sessions.session_attribution.medium = '(not set)')
                  then 'Direct'
                when sessions.session_attribution.medium = 'organic'
                  then 'Organic Search'
                when REGEXP_CONTAINS(sessions.session_attribution.source, r"^(facebook|instagram|pinterest|reddit|twitter|linkedin)") = true
                 and REGEXP_CONTAINS(sessions.session_attribution.medium, r"^(cpc|ppc|paid)") = true
                  then 'Paid Social'
                when REGEXP_CONTAINS(sessions.session_attribution.source, r"^(facebook|instagram|pinterest|reddit|twitter|linkedin)") = true
                  or REGEXP_CONTAINS(sessions.session_attribution.medium, r"^(social|social-network|social-media|sm|social network|social media)") = true
                  then 'Organic Social'
                when REGEXP_CONTAINS(sessions.session_attribution.medium, r"email|e-mail|e_mail|e mail") = true
                  or REGEXP_CONTAINS(sessions.session_attribution.source, r"email|e-mail|e_mail|e mail") = true
                  then 'Email'
                when REGEXP_CONTAINS(sessions.session_attribution.medium, r"affiliate|affiliates") = true
                  then 'Affiliates'
                when sessions.session_attribution.medium = 'referral'
                  then 'Referral'
                when REGEXP_CONTAINS(sessions.session_attribution.medium, r"^(cpc|ppc|paidsearch)$")
                  then 'Paid Search'
                when REGEXP_CONTAINS(sessions.session_attribution.medium, r"^(display|cpm|banner)$")
                  then 'Display'
                when REGEXP_CONTAINS(sessions.session_attribution.medium, r"^(cpv|cpa|cpp|content-text)$")
                  then 'Other Advertising'
                else '(Other)' end  AS sessions_session_attribution_channel,
    sessions.session_attribution.page_referrer  AS sessions_session_attribution_page_referrer,
    sessions.session_attribution.campaign  AS sessions_session_attribution_campaign,
    sessions.ga_session_number  AS sessions_ga_session_number,
    sessions.device_data.device__language  AS sessions_device_data_language,
    sessions.device_data.device__web_info_browser  AS sessions_device_data_web_info_browser,
    sessions.device_data.device__operating_system  AS sessions_device_data_operating_system,
    sessions.device_data.device__category  AS sessions_device_data_device_category,
    sessions.geo_data.geo__city  AS sessions_geo_data_city,
    sessions.geo_data.geo__country  AS sessions_geo_data_country,
    (select coalesce(regexp_extract((select value.string_value from UNNEST(event_params) where key = "page_location"),r"(?:.*?[\.][^\/]*)([\/][^\?]+)"),'/')
          from UNNEST(sessions.event_data) as event_history
          where event_history.sl_key = (sessions.sl_key) and event_history.page_view_rank = 1 limit 1)  AS sessions_landing_page,
    sessions.device_data.device__web_info_hostname  AS sessions_device_data_web_info_hostname,
    (select coalesce(regexp_extract((select value.string_value from UNNEST(event_params) where key = "page_location"),r"(?:.*?[\.][^\/]*)([\/][^\?]+)"),'/')
          from UNNEST(sessions.event_data) as event_history
          where event_history.sl_key = (sessions.sl_key) and event_history.page_view_reverse_rank = 1 limit 1)  AS sessions_exit_page,
        (CASE WHEN sessions.session_data.session_page_view_count = 1  THEN 'Yes' ELSE 'No' END) AS events_is_bounce,
    sessions.session_data.session_event_count  AS sessions_session_data_session_event_count,
    sessions.session_data.engaged_events  AS sessions_session_data_engaged_events,
    sessions.device_data.device__web_info_browser_version  AS sessions_device_data_web_info_browser_version,
    sessions.device_data.device__operating_system_version  AS sessions_device_data_operating_system_version,
    sessions.geo_data.geo__region  AS sessions_geo_data_region,
    COUNT(DISTINCT sessions.sl_key ) AS sessions_total_sessions,
        COUNT(DISTINCT CASE WHEN sessions.session_data.is_first_visit_session  THEN sessions.sl_key  ELSE NULL END) / nullif(COUNT(DISTINCT sessions.sl_key ), 0) AS sessions_total_first_visit_sessions_percentage,
    COUNT(DISTINCT CASE WHEN sessions.session_data.is_first_visit_session  THEN sessions.sl_key  ELSE NULL END) AS sessions_total_first_visit_sessions,
        COUNT(DISTINCT CASE WHEN sessions.session_data.is_engaged_session  THEN sessions.sl_key  ELSE NULL END) / nullif(COUNT(DISTINCT sessions.sl_key ), 0) AS sessions_total_engaged_sessions_percentage,
    COUNT(DISTINCT CASE WHEN sessions.session_data.is_engaged_session  THEN sessions.sl_key  ELSE NULL END) AS sessions_total_engaged_sessions,
    COUNT(DISTINCT CASE WHEN ((TIMESTAMP_DIFF(sessions.session_data.session_end, sessions.session_data.session_start, second))/86400.0) = 0  THEN sessions.sl_key  ELSE NULL END) AS sessions_total_bounced_sessions,
        COUNT(DISTINCT CASE WHEN ((TIMESTAMP_DIFF(sessions.session_data.session_end, sessions.session_data.session_start, second))/86400.0) = 0  THEN sessions.sl_key  ELSE NULL END) / nullif(COUNT(DISTINCT sessions.sl_key ), 0) AS sessions_total_bounced_sessions_percentage,
    (ROUND(COALESCE(CAST( ( SUM(DISTINCT (CAST(ROUND(COALESCE( ( ((TIMESTAMP_DIFF(sessions.session_data.session_end, sessions.session_data.session_start, second))/86400.0)   )  ,0)*(1/1000*1.0), 9) AS NUMERIC) + (cast(cast(concat('0x', substr(to_hex(md5(CAST( sessions.sl_key   AS STRING))), 1, 15)) as int64) as numeric) * 4294967296 + cast(cast(concat('0x', substr(to_hex(md5(CAST( sessions.sl_key   AS STRING))), 16, 8)) as int64) as numeric)) * 0.000000001 )) - SUM(DISTINCT (cast(cast(concat('0x', substr(to_hex(md5(CAST( sessions.sl_key   AS STRING))), 1, 15)) as int64) as numeric) * 4294967296 + cast(cast(concat('0x', substr(to_hex(md5(CAST( sessions.sl_key   AS STRING))), 16, 8)) as int64) as numeric)) * 0.000000001) )  / (1/1000*1.0) AS FLOAT64), 0), 6) / NULLIF(CAST(COUNT(DISTINCT CASE WHEN   ( ((TIMESTAMP_DIFF(sessions.session_data.session_end, sessions.session_data.session_start, second))/86400.0)   )   IS NOT NULL THEN  sessions.sl_key   ELSE NULL END) AS FLOAT64), 0.0)) AS sessions_average_session_duration,
    (ROUND(COALESCE(CAST( ( SUM(DISTINCT (CAST(ROUND(COALESCE( sessions.session_data.session_page_view_count  ,0)*(1/1000*1.0), 9) AS NUMERIC) + (cast(cast(concat('0x', substr(to_hex(md5(CAST( sessions.sl_key   AS STRING))), 1, 15)) as int64) as numeric) * 4294967296 + cast(cast(concat('0x', substr(to_hex(md5(CAST( sessions.sl_key   AS STRING))), 16, 8)) as int64) as numeric)) * 0.000000001 )) - SUM(DISTINCT (cast(cast(concat('0x', substr(to_hex(md5(CAST( sessions.sl_key   AS STRING))), 1, 15)) as int64) as numeric) * 4294967296 + cast(cast(concat('0x', substr(to_hex(md5(CAST( sessions.sl_key   AS STRING))), 16, 8)) as int64) as numeric)) * 0.000000001) )  / (1/1000*1.0) AS FLOAT64), 0), 6) / NULLIF(CAST(COUNT(DISTINCT CASE WHEN   sessions.session_data.session_page_view_count   IS NOT NULL THEN  sessions.sl_key   ELSE NULL END) AS FLOAT64), 0.0)) AS sessions_average_page_views_per_session,
       COUNT(DISTINCT sessions.user_pseudo_id ) AS sessions_total_users,
    COUNT(DISTINCT CASE WHEN NOT COALESCE(sessions.session_data.is_first_visit_session , FALSE) THEN sessions.user_pseudo_id  ELSE NULL END) AS sessions_total_returning_users,
    COUNT(DISTINCT CASE WHEN sessions.session_data.is_first_visit_session  THEN sessions.user_pseudo_id  ELSE NULL END) AS sessions_total_new_users,
        COUNT(DISTINCT CASE WHEN NOT COALESCE(sessions.session_data.is_first_visit_session , FALSE) THEN sessions.user_pseudo_id  ELSE NULL END) / nullif(COUNT(DISTINCT sessions.user_pseudo_id ), 0) AS sessions_percentage_returning_users,
        COUNT(DISTINCT CASE WHEN sessions.session_data.is_first_visit_session  THEN sessions.user_pseudo_id  ELSE NULL END) / nullif(COUNT(DISTINCT sessions.user_pseudo_id ), 0) AS sessions_percentage_new_users,

FROM `<yourProject.yourdataset>.sessions_*` AS sessions

-- use case of date variables shown below is for Data Studio
WHERE _table_suffix between @DS_START_DATE AND @DS_END_DATE

GROUP BY
    1,
    2,
    3,
    4,
    5,
    6,
    7,
    8,
    9,
    10,
    11,
    12,
    13,
    14,
    15,
    16,
    17,
    18,
    19,
    20,
    21,
    22,
    23,
    24,
    25,
    26,
    27,
    28,
    29,
    30,
    31,
    32,
    33,
    34,
    35,
    36,
    37,
    38
   
ORDER BY
    7 DESC
