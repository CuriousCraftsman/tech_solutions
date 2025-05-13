/*
 * Purpose: Create an SCD Type 2 dimension table to track the closed and open periods for a company's pizza stores
 *
 * Note: An outlet is considered “closed” if it has had no transactions for 30 consecutive days. Outlets can re-open.
 * */


WITH current_and_previous_trans_date AS (
	-- we need this to find the days elapsed between transactions to determine if a shop is closed or not
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
        'open' AS status, -- all transaction records indicate the shop is active, so we set them all to open
        (curr_trans_date - prev_trans_date) AS days_since_last_trans
    FROM current_and_previous_trans_date
),

closed_dates AS (
	-- setting the closed period to the period between recorded transaction dates
    SELECT 
        shop_id, 
        /* these are not transaction dates since the store was closed, but we treat 
           them that way to make it easier to set the valid_to and from dates later */
        (prev_trans_date + 1) AS curr_trans_date, -- date the closed period began
        (curr_trans_date - 1) AS prev_trans_date, -- date the closed period ended
        'closed' AS status,  
        0 AS days_since_last_trans  
    FROM days_since_prev_trans
    WHERE days_since_last_trans >= 30 -- a closed outlet is one that has no transactions for at least 30 consecutive days
),


open_or_closed AS (
    SELECT * FROM closed_dates 
    UNION 
    SELECT * FROM days_since_prev_trans
),


status_change AS (
	-- track the status change so we can group on this later to set valid_to and from dates
    SELECT 
        shop_id, 
        status,
        LAG(status) OVER (PARTITION BY shop_id ORDER BY curr_trans_date) AS prev_status,
        curr_trans_date
    FROM open_or_closed
),

status_change_indicated AS (
	/*  when shops go from open to closed periods and vice versa, each period will have a 
		unique streak number attached for all dates during that period */
    SELECT 
        shop_id, 
        status, 
        curr_trans_date,
        SUM(CASE WHEN status <> prev_status THEN 1 ELSE 0 END) 
            OVER (PARTITION BY shop_id ORDER BY curr_trans_date) AS status_change_streak
    FROM status_change
),

max_status_change_indicated AS (	
	-- we'll use the max_change_streak to identify the latest records and mark them as current by setting the date to 9999-12-31
	SELECT 
        *, 
        MAX(status_change_streak) OVER (PARTITION BY shop_id) AS max_status_change_streak 
    FROM status_change_indicated 
),


lead_trans_date AS (
	-- using this later to set the valid_to date 
	select 
        *,
	    LEAD(curr_trans_date) OVER (PARTITION BY shop_id ORDER BY curr_trans_date) AS next_trans_date
	FROM max_status_change_indicated
),

dim_shop AS (
	-- creates the SCD type 2 dimension table
    SELECT  
        shop_id, 
        status, 
        MIN(curr_trans_date) AS valid_from,
        CASE 
            WHEN status_change_streak = max_status_change_streak THEN '9999-12-31' -- to mark when it's the latest or 'active' record
            ELSE MAX(next_trans_date) - 1 -- inactive records will have an end date of one day prior to the next record's start date
        END AS valid_to
    FROM lead_trans_date
    GROUP BY shop_id, status, status_change_streak, max_status_change_streak
    ORDER BY shop_id, valid_from
)

SELECT * FROM dim_shop