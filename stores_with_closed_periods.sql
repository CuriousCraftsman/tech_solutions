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
        'open' AS status,
        (curr_trans_date - prev_trans_date) AS days_since_last_trans
    FROM current_and_previous_trans_date
),

-- setting the closed period to the period between recorded transaction dates
closed_dates AS (
    SELECT 
        shop_id, 
        (prev_trans_date + 1) AS curr_trans_date, -- date the closed period began
        (curr_trans_date - 1) AS prev_trans_date, -- date the closed period ended
        'closed' AS status, 
        0 AS days_since_last_trans  
    FROM days_since_prev_trans
    WHERE days_since_last_trans >= 30
),


open_or_closed AS (
    SELECT * FROM closed_dates 
    UNION 
    SELECT * FROM days_since_prev_trans
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
        SUM(CASE WHEN status <> prev_status THEN 1 ELSE 0 END) 
            OVER (PARTITION BY shop_id ORDER BY curr_trans_date) AS status_change_streak
    FROM status_change
),

max_status_change_indicated AS (	
	SELECT 
        *, 
        MAX(status_change_streak) OVER (PARTITION BY shop_id) AS max_status_change_streak 
    FROM status_change_indicated 
),


-- using this later to set the valid_to date 
lead_trans_date AS (
	select 
        *,
	    LEAD(curr_trans_date) OVER (PARTITION BY shop_id ORDER BY curr_trans_date) AS next_trans_date
	FROM max_status_change_indicated
),

dim_shop AS (
    SELECT  
        shop_id, 
        status, 
        MIN(curr_trans_date) AS valid_from,
        CASE 
            WHEN status_change_streak = max_status_change_streak THEN '9999-12-31'
            ELSE MAX(next_trans_date) - 1
        END AS valid_to
    FROM lead_trans_date
    GROUP BY shop_id, status, status_change_streak, max_status_change_streak
    ORDER BY shop_id, valid_from
)

SELECT 
    COUNT(DISTINCT shop_id) 
FROM dim_shop
WHERE status = 'closed'