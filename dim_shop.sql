WITH current_and_previous_trans_date AS (
    SELECT 
        shop_id, 
        date AS curr_trans_date, 
        LAG(date) OVER (PARTITION BY shop_id ORDER BY date) AS prev_trans_date
    FROM  public.sample  
),

days_since_prev_trans AS (
    SELECT shop_id,
        curr_trans_date,
        prev_trans_date,
        (curr_trans_date - prev_trans_date) AS days_since_last_trans
    FROM current_and_previous_trans_date
),

open_or_closed AS (
    SELECT 
        shop_id, 
        curr_trans_date,
        CASE 
            WHEN days_since_last_trans >= 30 THEN 'closed' 
            ELSE 'open' 
        END AS status
    FROM days_since_prev_trans
),

status_change AS (
    SELECT 
        shop_id, 
        status,
        LAG(status) OVER (PARTITION BY shop_id ORDER BY curr_trans_date) AS prev_status,
        curr_trans_date
    FROM open_or_closed
),

status_change_indicated AS (
    SELECT 
        shop_id, 
        status, 
        curr_trans_date,
        SUM(CASE WHEN status <> prev_status THEN 1 ELSE null END) 
            OVER (PARTITION BY shop_id ORDER BY curr_trans_date) AS status_change_streak
    FROM status_change
),

max_status_change_indicated as ( 
 select *, MAX(status_change_streak) over (partition by shop_id) as max_status_change_streak from status_change_indicated 
),

dim_shop AS (
 SELECT  
        shop_id, 
        status, 
        MIN(curr_trans_date) AS valid_from,
        case 
         when status_change_streak = max_status_change_streak then '9999-12-31'
         else MAX(curr_trans_date)
        end AS valid_to
    FROM max_status_change_indicated
    GROUP BY shop_id, status, status_change_streak, max_status_change_streak
    ORDER BY shop_id, valid_from
)

select * from dim_shop
 