-------------------------------------
-- All credit goes to the Looker dev team for the approach below.
-- This sql transforms events_ tables into sessions_ tables.
-- I have set this up as a daily scheduled query partitioned by partition_date
-- You can configure your destination table as: sessions_{run_time-48h|'%Y%m%d'}
-- Note I decided to wait a couple of days believing events_ tables are not truly finalized
-- prove me wrong or update the scheduled query config and the WHERE sql below as you see fit
-------------------------------------
-- the folowing 3 lines are handled by the scheduler interface. You get the idea.
-- create or replace table `<yourProject.yourdataset>.sessions_`
-- partition by partition_date
-- as (
-------------------------------------

    with
      -- obtains a list of sessions, uniquely identified by the table date, ga_session_id event parameter, ga_session_number event parameter, and the user_pseudo_id.
      session_list_with_event_history as (
        select PARSE_DATE('%Y%m%d', _table_suffix) as partition_date
            ,   timestamp(PARSE_DATE('%Y%m%d', REGEXP_EXTRACT(_TABLE_SUFFIX,r'[0-9]+'))) session_date
            ,  (select value.int_value from UNNEST(events.event_params) where key = "ga_session_id") ga_session_id
            ,  (select value.int_value from UNNEST(events.event_params) where key = "ga_session_number") ga_session_number
            ,  events.user_pseudo_id
            -- unique key for session:
            ,  timestamp(PARSE_DATE('%Y%m%d', REGEXP_EXTRACT(_TABLE_SUFFIX,r'[0-9]+')))||(select value.int_value from UNNEST(events.event_params) where key = "ga_session_id")||(select value.int_value from UNNEST(events.event_params) where key = "ga_session_number")||events.user_pseudo_id sl_key
            ,  row_number() over (partition by (timestamp(PARSE_DATE('%Y%m%d', REGEXP_EXTRACT(_TABLE_SUFFIX,r'[0-9]+')))||(select value.int_value from UNNEST(events.event_params) where key = "ga_session_id")||(select value.int_value from UNNEST(events.event_params) where key = "ga_session_number")||events.user_pseudo_id) order by events.event_timestamp) event_rank
            ,  (TIMESTAMP_DIFF(TIMESTAMP_MICROS(LEAD(events.event_timestamp) OVER (PARTITION BY timestamp(PARSE_DATE('%Y%m%d', REGEXP_EXTRACT(_TABLE_SUFFIX,r'[0-9]+')))||(select value.int_value from UNNEST(events.event_params) where key = "ga_session_id")||(select value.int_value from UNNEST(events.event_params) where key = "ga_session_number")||events.user_pseudo_id ORDER BY events.event_timestamp asc))
               ,TIMESTAMP_MICROS(events.event_timestamp),second)/86400.0) time_to_next_event
            , case when events.event_name = 'page_view' then row_number() over (partition by (timestamp(PARSE_DATE('%Y%m%d', REGEXP_EXTRACT(_TABLE_SUFFIX,r'[0-9]+')))||(select value.int_value from UNNEST(events.event_params) where key = "ga_session_id")||(select value.int_value from UNNEST(events.event_params) where key = "ga_session_number")||events.user_pseudo_id), case when events.event_name = 'page_view' then true else false end order by events.event_timestamp)
              else 0 end as page_view_rank
            , case when events.event_name = 'page_view' then row_number() over (partition by (timestamp(PARSE_DATE('%Y%m%d', REGEXP_EXTRACT(_TABLE_SUFFIX,r'[0-9]+')))||(select value.int_value from UNNEST(events.event_params) where key = "ga_session_id")||(select value.int_value from UNNEST(events.event_params) where key = "ga_session_number")||events.user_pseudo_id), case when events.event_name = 'page_view' then true else false end order by events.event_timestamp desc)
              else 0 end as page_view_reverse_rank
            , case when events.event_name = 'page_view' then (TIMESTAMP_DIFF(TIMESTAMP_MICROS(LEAD(events.event_timestamp) OVER (PARTITION BY timestamp(PARSE_DATE('%Y%m%d', REGEXP_EXTRACT(_TABLE_SUFFIX,r'[0-9]+')))||(select value.int_value from UNNEST(events.event_params) where key = "ga_session_id")||(select value.int_value from UNNEST(events.event_params) where key = "ga_session_number")||events.user_pseudo_id , case when events.event_name = 'page_view' then true else false end ORDER BY events.event_timestamp asc))
               ,TIMESTAMP_MICROS(events.event_timestamp),second)/86400.0) else null end as time_to_next_page -- this window function yields 0 duration results when session page_view count = 1.
            -- raw event data:
            , events.event_date
            , events.event_timestamp
            , events.event_name
            , events.event_params
            , events.event_previous_timestamp
            , events.event_value_in_usd
            , events.event_bundle_sequence_id
            , events.event_server_timestamp_offset
            , events.user_id
            -- , events.user_pseudo_id
            , events.user_properties
            , events.user_first_touch_timestamp
            , events.user_ltv
            , events.device
            , events.geo
            , events.app_info
            , events.traffic_source
            , events.stream_id
            , events.platform
            , events.event_dimensions
            , events.ecommerce
            , events.items
              from `<yourProject.yourdataset>.events_*` events
              
              -- change your interval as needed
              where timestamp(PARSE_DATE('%Y%m%d', REGEXP_EXTRACT(_TABLE_SUFFIX,r'[0-9]+'))) = ((TIMESTAMP_ADD(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY), INTERVAL -2 DAY)))
              
        ),

      -- Session-Level Facts, session start, end, duration
      session_facts as (
        select sl.sl_key
            ,  COUNT(sl.event_timestamp) session_event_count
            ,  SUM(case when sl.event_name = 'page_view' then 1 else 0 end) session_page_view_count
            ,  COALESCE(SUM((select value.int_value from UNNEST(sl.event_params) where key = "engaged_session_event")),0) engaged_events
            ,  case when (COALESCE(SUM((select value.int_value from UNNEST(sl.event_params) where key = "engaged_session_event")),0) = 0
                     and COALESCE(SUM((select coalesce(cast(value.string_value as INT64),value.int_value) from UNNEST(sl.event_params) where key = "session_engaged"))) = 0)
                    then false else true end as is_engaged_session
                  , case when countif(event_name = 'first_visit') = 0 then false else true end as is_first_visit_session
                  , MAX(TIMESTAMP_MICROS(sl.event_timestamp)) as session_end
                  , MIN(TIMESTAMP_MICROS(sl.event_timestamp)) as session_start
                  , (MAX(sl.event_timestamp) - MIN(sl.event_timestamp))/(60 * 1000 * 1000) AS session_length_minutes
        from session_list_with_event_history sl
        group by 1
        ),

      -- Retrieves the last non-direct medium, source, and campaign from the session's page_view events.
      session_tags as (
        select distinct sl.sl_key
            ,  first_value((select value.string_value from unnest(sl.event_params) where key = 'medium')) over (partition by sl.sl_key order by sl.event_timestamp desc) medium
            ,  first_value((select value.string_value from unnest(sl.event_params) where key = 'source')) over (partition by sl.sl_key order by sl.event_timestamp desc) source
            ,  first_value((select value.string_value from unnest(sl.event_params) where key = 'campaign')) over (partition by sl.sl_key order by sl.event_timestamp desc) campaign
            ,  first_value((select value.string_value from unnest(sl.event_params) where key = 'page_referrer')) over (partition by sl.sl_key order by sl.event_timestamp desc) page_referrer
        from session_list_with_event_history sl
        where sl.event_name in ('page_view')
          and (select value.string_value from unnest(sl.event_params) where key = 'medium') is not null -- NULL medium is direct, filtering out nulls to ensure last non-direct.
        ),

      -- Device and Geo Columns from 'Session Start' event.
      device_geo as (
        select sl.sl_key
            ,  sl.device.category device__category
            ,  sl.device.mobile_brand_name device__mobile_brand_name
            ,  sl.device.mobile_model_name device__mobile_model_name
            ,  sl.device.mobile_brand_name||' '||device.mobile_model_name device__mobile_device_info
            ,  sl.device.mobile_marketing_name device__mobile_marketing_name
            ,  sl.device.mobile_os_hardware_model device__mobile_os_hardware_model
            ,  sl.device.operating_system device__operating_system
            ,  sl.device.operating_system_version device__operating_system_version
            ,  sl.device.vendor_id device__vendor_id
            ,  sl.device.advertising_id device__advertising_id
            ,  sl.device.language device__language
            ,  sl.device.time_zone_offset_seconds device__time_zone_offset_seconds
            ,  sl.device.is_limited_ad_tracking device__is_limited_ad_tracking
            ,  sl.device.web_info.browser device__web_info_browser
            ,  sl.device.web_info.browser_version device__web_info_browser_version
            #,  sl.device.web_info.hostname device__web_info_hostname
            ,  case when sl.device.category = 'mobile' then true else false end as device__is_mobile
            ,  sl.geo.continent geo__continent
            ,  sl.geo.country geo__country
            ,  sl.geo.city geo__city
            ,  sl.geo.metro geo__metro
            ,  sl.geo.sub_continent geo__sub_continent
            ,  sl.geo.region geo__region
        from session_list_with_event_history sl
        where sl.event_name = 'session_start'
        ),

      -- Packs the event-level data into an array of structs, leaving a session-level row.
      session_event_packing as (
        select sl.partition_date
            ,  sl.session_date session_date
            ,  sl.ga_session_id ga_session_id
            ,  sl.ga_session_number ga_session_number
            ,  sl.user_pseudo_id user_pseudo_id
            ,  sl.sl_key
            ,  ARRAY_AGG(STRUCT(  sl.sl_key
                                , sl.event_rank
                                , sl.page_view_rank
                                , sl.page_view_reverse_rank
                                , sl.time_to_next_event
                                , sl.time_to_next_page
                                , sl.event_date
                                , sl.event_timestamp
                                , sl.event_name
                                , sl.event_params
                                , sl.event_previous_timestamp
                                , sl.event_value_in_usd
                                , sl.event_bundle_sequence_id
                                , sl.event_server_timestamp_offset
                                , sl.user_id
                                , sl.user_pseudo_id
                                , sl.user_properties
                                , sl.user_first_touch_timestamp
                                , sl.user_ltv
                                , sl.device
                                , sl.geo
                                , sl.app_info
                                , sl.traffic_source
                                , sl.stream_id
                                , sl.platform
                                , sl.event_dimensions
                                , sl.ecommerce
                                , sl.items)) event_data
        from session_list_with_event_history sl
        group by 1,2,3,4,5,6
        )

      -- Final Select Statement:
      select se.partition_date
          ,  se.session_date session_date
          ,  se.ga_session_id ga_session_id
          ,  se.ga_session_number ga_session_number
          ,  se.user_pseudo_id user_pseudo_id
          ,  se.sl_key
          -- packing session-level data into structs by category
          ,  (SELECT AS STRUCT coalesce(sa.medium,'(none)') medium -- sessions missing last-non-direct are direct
                            ,  coalesce(sa.source,'(direct)') source
                            ,  coalesce(sa.campaign,'(direct)') campaign
                            ,  sa.page_referrer) session_attribution
          ,  (SELECT AS STRUCT sf.session_event_count
                            ,  sf.engaged_events
                            ,  sf.session_page_view_count
                            ,  sf.is_engaged_session
                            ,  sf.is_first_visit_session
                            ,  sf.session_end
                            ,  sf.session_start
                            ,  sf.session_length_minutes) session_data
          ,  (SELECT AS STRUCT d.device__category
                            ,  d.device__mobile_brand_name
                            ,  d.device__mobile_model_name
                            ,  d.device__mobile_device_info
                            ,  d.device__mobile_marketing_name
                            ,  d.device__mobile_os_hardware_model
                            ,  d.device__operating_system
                            ,  d.device__operating_system_version
                            ,  d.device__vendor_id
                            ,  d.device__advertising_id
                            ,  d.device__language
                            ,  d.device__time_zone_offset_seconds
                            ,  d.device__is_limited_ad_tracking
                            ,  d.device__web_info_browser
                            ,  d.device__web_info_browser_version
                            #,  d.device__web_info_hostname
                            ,  d.device__is_mobile) device_data
          ,  (SELECT AS STRUCT d.geo__continent
                            ,  d.geo__country
                            ,  d.geo__city
                            ,  d.geo__metro
                            ,  d.geo__sub_continent
                            ,  d.geo__region) geo_data
          ,  se.event_data event_data
      from session_event_packing se
      left join session_tags sa
        on  se.sl_key = sa.sl_key
      left join session_facts sf
        on  se.sl_key = sf.sl_key
      left join device_geo d
        on  se.sl_key = d.sl_key

-------------------------------------
--)
