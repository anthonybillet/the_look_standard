
view: dim_trial_to_paid_conversions {
  derived_table: {
    sql: with transactions as (
                select
                    transaction_date,
                    user_id,
                    os,
                    signup_country as country,
                    trial_start_time,
                    trial_end_time,

                    purchase_type,
                    purchase_start_time,
                    purchase_end_time,

                    product as subscription_interval,
                    tier,

                    refund_status,
                    unsubscribe_status
                from core.fct_user_transactions
                where purchase_type in ('trial', 'new')
                  and transaction_date >= '2023-01-01' -- temporary
            )
            , trial as (
              select
                  *,
                  row_number() over (partition by user_id order by trial_start_time asc) as trial_record_order
              from transactions
              where purchase_type = 'trial'
                  and date_diff('day', trial_start_time, trial_end_time) > 6 --remove weird short trials
            )
            , paid as (
              select
                  *,
                  row_number() over (partition by user_id order by transaction_date asc) as conversion_record_order
              from transactions
              where purchase_type = 'new'
                  and refund_status = false
            )
            , trial_to_paid as (
                select
                    trial.user_id,
                    trial.trial_start_time,
                    trial.trial_end_time,
                    trial.tier,
                    trial.subscription_interval,
                    trial.os,

                    paid.purchase_start_time,
                    paid.purchase_end_time,
                    paid.tier as paid_tier,


                    paid.refund_status,
                    paid.unsubscribe_status
                from trial
                left join paid
                  on trial.user_id = paid.user_id
                where (conversion_record_order = 1 or conversion_record_order is null) -- deduplicate, but allow users with no conversion
                  and trial_record_order = 1 -- deduplicate
            )

            , creation_trial_paid as (
              select
                trial_to_paid.user_id,
                users.signup_timestamp,
                trial_to_paid.trial_start_time,
                trial_to_paid.trial_end_time,
                trial_to_paid.purchase_start_time,
                trial_to_paid.subscription_interval,
                trial_to_paid.tier,
                trial_to_paid.refund_status,
                trial_to_paid.unsubscribe_status,
                trial_to_paid.os,

                -- month cohorts
                users.signup_month,
                date_trunc('month', trial_to_paid.trial_start_time) as trial_month,
                date_trunc('month', trial_to_paid.purchase_start_time) as purchase_month,

                -- time deltas
                datediff(month, users.signup_month, trial_to_paid.trial_start_time) as signup_to_trial_months,
                datediff(month, users.signup_month, trial_to_paid.purchase_start_time) as signup_to_paid_months,

                datediff(day, trial_to_paid.trial_start_time, trial_to_paid.purchase_start_time) as trial_start_to_paid_days,
                datediff(day, trial_to_paid.trial_end_time, trial_to_paid.purchase_start_time) as trial_end_to_paid_days

              from trial_to_paid
              left join  core_ext.fct_new_users users
                on trial_to_paid.user_id = users.user_id
              where users.signup_timestamp is not null
            )
      , aggregated as (
          select
              trial_month::date,

              date_diff('month', signup_month, trial_month) as account_age_months_at_trial_start,
              signup_month,
              --purchase_month::date,
              --subscription_interval,
              tier,
              os,
              count(*) as all_users,
              count(case
                      when trial_month is not null and purchase_month is null
                      then user_id else null
                  end) as users_trials_did_not_convert,
              count(case
                      when trial_month is not null and purchase_month is not null
                      then user_id else null
                  end) as users_trials_did_convert

          from creation_trial_paid
          group by
              trial_month,
              --purchase_month,
              signup_month,
              --subscription_interval,
              tier,
              os
      )
      -- uniqueness check
      -- select user_id, count(*) as cnt from creation_trial_paid group by user_id having count(*) > 1 order by cnt desc

      select
          *,
          (users_trials_did_convert::numeric)/(users_trials_did_convert+users_trials_did_not_convert)::numeric as trial_paid_cvr
      from aggregated
      order by trial_month, account_age_months_at_trial_start, trial_month, tier, os --subscription_interval ;;
  }

  measure: count {
    type: count
    drill_fields: [detail*]
  }

  dimension: trial_month {
    type: date
    sql: ${TABLE}.trial_month ;;
  }

  dimension: account_age_months_at_trial_start {
    type: number
    sql: ${TABLE}.account_age_months_at_trial_start ;;
  }

  dimension: signup_month {
    type: date
    sql: ${TABLE}.signup_month ;;
  }

  dimension: tier {
    type: string
    sql: ${TABLE}.tier ;;
  }

  dimension: os {
    type: string
    sql: ${TABLE}.os ;;
  }

  dimension: all_users {
    type: number
    sql: ${TABLE}.all_users ;;
  }

  dimension: users_trials_did_not_convert {
    type: number
    sql: ${TABLE}.users_trials_did_not_convert ;;
  }

  dimension: users_trials_did_convert {
    type: number
    sql: ${TABLE}.users_trials_did_convert ;;
  }

  dimension: trial_paid_cvr {
    type: number
    sql: ${TABLE}.trial_paid_cvr ;;
  }

  set: detail {
    fields: [
        trial_month,
  account_age_months_at_trial_start,
  signup_month,
  tier,
  os,
  all_users,
  users_trials_did_not_convert,
  users_trials_did_convert,
  trial_paid_cvr
    ]
  }
}
