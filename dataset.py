#

sql_build = '''
WITH users_and_camp AS
(
    SELECT
        *
    FROM
        (
            SELECT
                csc.campaign_id,
                csc.segment_type,
                csc.segment_id,
                csc.targetGoal,
                (
                    CASE
                        WHEN segment_type = 'TEST' AND first_interaction_executed_at IS NULL THEN NULL
                        ELSE us.user_id
                    END
                ) AS user_id,
                coolingPeriodEnd,
                estimationPeriodEnd,
                first_interaction_executed_at,
                TIMESTAMP_ADD(first_interaction_executed_at, INTERVAL 7 DAY) AS first_interaction_executed_at_7d
            FROM
                bridge.rep_camp_seg_creative AS csc
            INNER JOIN
                raw.belkacar_mysql_rep_user_2_segment AS us
            ON
                csc.segment_id = us.segment_id
            LEFT JOIN
                (
                    SELECT
                        user_id,
                        campaign_id,
                        MIN(executed_at) AS first_interaction_executed_at
                    FROM
                        raw.belkacar_mysql_rep_interaction_task_log
                    WHERE
                        status = 'DONE'
                    GROUP BY
                        1, 2
                ) AS ui
            ON
                us.user_id = ui.user_id
            AND
                ui.campaign_id = csc.campaign_id
        )
    WHERE
        user_id IS NOT NULL
),
avg_hours_to_control AS
(
    SELECT
        campaign_id,
        CAST(
            AVG(
                TIMESTAMP_DIFF(
                    (CASE WHEN first_interaction_executed_at_7d >= estimationPeriodEnd
                        THEN estimationPeriodEnd ELSE first_interaction_executed_at_7d END),
                    first_interaction_executed_at,
                HOUR)
            )
        AS INT64) AS hours_to_end_of_control,
        MIN(first_interaction_executed_at) AS first_executed_from_camp_at
    FROM
        users_and_camp
    WHERE
        segment_type = 'TEST'
    GROUP BY
        1
),
user_and_time_exp AS
(
    SELECT DISTINCT
        campaign_id,
        segment_type,
        targetGoal,
        segment_id,
        user_id,
        (
            CASE
                WHEN segment_type = 'TEST' THEN first_interaction_executed_at
                ELSE coolingPeriodEnd
            END
        ) AS started_at,
        (
            CASE
                WHEN segment_type = 'TEST' THEN test_window_finished_at
                ELSE control_window_finished_at
            END
        ) AS finished_at
    FROM
        (
            SELECT
                uc.campaign_id,
                uc.segment_type,
                uc.segment_id,
                uc.targetGoal,
                uc.user_id,
                (
                    CASE
                        WHEN uc.coolingPeriodEnd <= h.first_executed_from_camp_at THEN h.first_executed_from_camp_at
                        ELSE uc.coolingPeriodEnd
                    END
                ) AS coolingPeriodEnd,
                (
                    CASE
                        WHEN TIMESTAMP_ADD(uc.coolingPeriodEnd, INTERVAL h.hours_to_end_of_control HOUR) >= uc.estimationPeriodEnd THEN uc.estimationPeriodEnd
                        ELSE TIMESTAMP_ADD(uc.coolingPeriodEnd, INTERVAL h.hours_to_end_of_control HOUR)
                    END
                ) AS control_window_finished_at,
                uc.first_interaction_executed_at,
                (
                    CASE
                        WHEN uc.first_interaction_executed_at_7d >= uc.estimationPeriodEnd THEN uc.estimationPeriodEnd
                        ELSE uc.first_interaction_executed_at_7d
                    END
                ) AS test_window_finished_at,
            FROM
                users_and_camp AS uc
            LEFT JOIN
                avg_hours_to_control AS h
            ON
                uc.campaign_id = h.campaign_id
            WHERE
                (
                    CASE
                        WHEN segment_type = 'TEST' AND first_interaction_executed_at IS NULL THEN FALSE
                        ELSE TRUE
                    END
                ) = TRUE
        )
),
user_with_app_and_rents AS
(
SELECT
    *,
    ROW_NUMBER() OVER(PARTITION BY campaign_id, segment_id, user_id ORDER BY session_started_at) AS rn_sessions,
    ROW_NUMBER() OVER(PARTITION BY campaign_id, segment_id, user_id ORDER BY rent_started_at) AS rn_rents,
FROM
    (
        SELECT
            ue.campaign_id,
            ue.segment_type,
            targetGoal,
            segment_id,
            ue.user_id,
            ue.started_at,
            ue.finished_at,
            al.demand_session_id,
            al.event_time AS session_started_at,
            r.rent_id,
            r.started_at AS rent_started_at
        FROM
            user_and_time_exp AS ue
        LEFT JOIN
            (
                SELECT DISTINCT
                    user_id,
                    event_time,
                    demand_session_id
                FROM
                    bi.demand_session
                WHERE
                    event IN ('APP_READY', 'APP_LAUNCH', 'CAR_OFFER')
            ) AS al
        ON
            ue.user_id = al.user_id
        AND
            al.event_time >= ue.started_at
        LEFT JOIN
            bi.rent AS r
        ON
            ue.user_id = r.user_id
        AND
            r.started_at >= ue.started_at
    )
)
SELECT
    a.campaign_id,
    a.segment_type,
    a.targetGoal,
    a.segment_id,
    COUNT(DISTINCT(a.user_id)) AS total_users,
    COUNT(DISTINCT(CASE
        WHEN LOWER(u.platform) = 'ios' THEN a.user_id
        ELSE NULL END)) AS total_users_ios,
    COUNT(DISTINCT(CASE
        WHEN LOWER(u.platform) = 'android' THEN a.user_id
        ELSE NULL END)) AS total_users_android,
    COUNT(DISTINCT(CASE
        WHEN (CASE
                WHEN num_of_app_launch < num_of_rents THEN num_of_rents
                ELSE num_of_app_launch END) > 0 THEN a.user_id
        ELSE NULL END)) AS app_openers,
    COUNT(DISTINCT(CASE
        WHEN LOWER(u.platform) = 'ios' THEN (CASE
                WHEN (CASE
                    WHEN num_of_app_launch_ios < num_of_rents_ios THEN num_of_rents_ios
                    ELSE num_of_app_launch_ios END) > 0 THEN a.user_id
                ELSE NULL END)
        ELSE NULL END)) AS app_openers_ios,
    COUNT(DISTINCT(CASE
        WHEN LOWER(u.platform) = 'android' THEN (CASE
                WHEN (CASE
                    WHEN num_of_app_launch_android < num_of_rents_android THEN num_of_rents_android
                    ELSE num_of_app_launch_android END) > 0 THEN a.user_id
                ELSE NULL END)
        ELSE NULL END)) AS app_openers_android,
    SUM(CASE WHEN num_of_app_launch < num_of_rents THEN num_of_rents ELSE num_of_app_launch END) AS num_of_app_launch_in_period,
    SUM(CASE WHEN num_of_app_launch_ios < num_of_rents_ios THEN num_of_rents_ios ELSE num_of_app_launch_ios END) AS num_of_app_launch_in_period_ios,
    SUM(CASE WHEN num_of_app_launch_android < num_of_rents_android THEN num_of_rents_android ELSE num_of_app_launch_android END) AS num_of_app_launch_in_period_android,
    AVG(time_to_first_session_in_period) AS avg_time_to_first_session_in_period,
    AVG(time_to_first_session) AS avg_time_to_first_session,
    COUNT(DISTINCT(CASE
            WHEN num_of_rents > 0 THEN a.user_id
        ELSE NULL END)) AS riders,
    COUNT(DISTINCT(CASE
            WHEN LOWER(u.platform) = 'ios' THEN (CASE
                WHEN num_of_rents > 0 THEN a.user_id
                ELSE NULL
            END)
        ELSE NULL END)) AS riders_ios,
    COUNT(DISTINCT(CASE
            WHEN LOWER(u.platform) = 'android' THEN (CASE
                WHEN num_of_rents > 0 THEN a.user_id
                ELSE NULL
            END)
        ELSE NULL END)) AS riders_android,
    SUM(num_of_rents) AS num_of_rents_in_period,
    SUM(num_of_rents_ios) AS num_of_rents_in_period_ios,
    SUM(num_of_rents_android) AS num_of_rents_in_period_android,
    AVG(time_to_first_rent_in_period) AS avg_time_to_first_rent_in_period,
    AVG(time_to_first_rent) AS avg_time_to_first_rent
FROM
    (
        SELECT
            a.campaign_id,
            a.segment_type,
            a.targetGoal,
            a.segment_id,
            a.user_id,
            COUNT(DISTINCT(CASE
                WHEN session_started_at <= finished_at THEN demand_session_id
                ELSE NULL END)) AS num_of_app_launch,
            COUNT(DISTINCT(CASE
                WHEN LOWER(u.platform) = 'ios' THEN (CASE
                    WHEN session_started_at <= finished_at THEN demand_session_id
                    ELSE NULL END)
                ELSE NULL END)) AS num_of_app_launch_ios,
            COUNT(DISTINCT(CASE
                WHEN LOWER(u.platform) = 'android' THEN (CASE
                    WHEN session_started_at <= finished_at THEN demand_session_id
                    ELSE NULL END)
                ELSE NULL END)) AS num_of_app_launch_android,
            COUNT(DISTINCT(CASE
                WHEN rent_started_at <= finished_at THEN rent_id
                ELSE NULL END)) AS num_of_rents,
            COUNT(DISTINCT(CASE
                WHEN LOWER(u.platform) = 'ios' THEN (CASE
                    WHEN rent_started_at <= finished_at THEN rent_id
                    ELSE NULL END)
                ELSE NULL END)) AS num_of_rents_ios,
            COUNT(DISTINCT(CASE
                WHEN LOWER(u.platform) = 'android' THEN (CASE
                    WHEN rent_started_at <= finished_at THEN rent_id
                    ELSE NULL END)
                ELSE NULL END)) AS num_of_rents_android,
            AVG(CASE
                WHEN rn_sessions = 1 AND session_started_at <= finished_at THEN TIMESTAMP_DIFF(session_started_at, started_at, MINUTE)
                ELSE NULL END) AS time_to_first_session_in_period,
            AVG(CASE
                WHEN rn_sessions = 1 THEN TIMESTAMP_DIFF(session_started_at, started_at, MINUTE)
                ELSE  NULL END) AS time_to_first_session,
            AVG(CASE
                WHEN rn_rents = 1 AND rent_started_at <= finished_at THEN TIMESTAMP_DIFF(rent_started_at, started_at, MINUTE)
                ELSE NULL END) AS time_to_first_rent_in_period,
            AVG(CASE
                WHEN rn_rents = 1 THEN TIMESTAMP_DIFF(rent_started_at, started_at, MINUTE)
                ELSE NULL END) AS time_to_first_rent
        FROM
            user_with_app_and_rents AS a
        LEFT JOIN
            bi.user AS u
        ON
            a.user_id = u.user_id
        GROUP BY
            1, 2, 3, 4, 5
    ) AS a
LEFT JOIN
    bi.user AS u
ON
    a.user_id = u.user_id
GROUP BY
  1, 2, 3, 4
ORDER BY
  1, 2, 3
'''
sql_testdf = """
SELECT
  campaign_id,
  segment_id,
  segment_type,
  total_users AS trials,
  CASE
    WHEN targetGoal = 'RENT_QUANTITY' THEN riders
  ELSE
  app_openers
END
  AS successes
FROM
  bridge.rep_segments_result
    """
