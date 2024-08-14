WITH
dates AS (
    SELECT
        :startDate AS start_date,
        :endDate AS end_date
),
start_date_with_days_count AS (
    SELECT
        start_date,
        EXPLODE(SEQUENCE(0, DATEDIFF(end_date, start_date) - 1, 1)) AS days_shift
    FROM dates
),
date_sequence AS (
    SELECT
        days_shift AS id,
        DATE_ADD(start_date, days_shift) AS full_date
    FROM start_date_with_days_count
),
date_dimension AS (
    SELECT
        id,
        full_date,
        CAST(DATE_FORMAT(full_date, 'yyyyMMdd') AS INT) AS date_key,
        DATE_FORMAT(full_date, 'MMMM dd, yyyy') AS full_date_description,
        DATE_FORMAT(full_date, 'yyyy-MM-dd') AS date_iso_8601,
        DATE_FORMAT(full_date, 'EEE') AS day_of_week_short_name,
        DATE_FORMAT(full_date, 'EEEE') AS day_of_week_name,
        CASE
            WHEN DAYOFWEEK(full_date) IN (1, 7) THEN 'Weekend'
            ELSE 'Weekday'
        END AS weekday_weekend,
        DAYOFWEEK(full_date) AS day_of_week,
        DAYOFMONTH(full_date) AS day_of_month,
        DAYOFYEAR(full_date) AS day_of_year,
        WEEKOFYEAR(full_date) AS week_of_year,
        DATE_TRUNC(full_date, 'YEAR') AS first_day_of_year_date,
        DATE_TRUNC(full_date, 'YEAR') = full_date AS is_first_day_of_year,
        DATE_TRUNC(full_date, 'MM') AS first_day_of_month_date,
        DATE_TRUNC(full_date, 'MM') = full_date AS is_first_day_of_month,
        DATE_TRUNC(full_date, 'QUARTER') AS first_day_of_quarter_date,
        DATE_TRUNC(full_date, 'QUARTER') = full_date AS is_first_day_of_quarter,
        LAST_DAY(full_date) AS last_day_of_month_date,
        LAST_DAY(full_date) = full_date AS is_last_day_of_month,
        DATE_FORMAT(full_date, 'MMM') AS month_name_short,
        DATE_FORMAT(full_date, 'MMMM') AS month_name,
        CONCAT('Q', QUARTER(full_date)) AS quarter_name,
        YEAR(full_date) AS calendar_year,
        QUARTER(full_date) AS calendar_quarter,
        MONTH(full_date) AS calendar_month,
        CONCAT(YEAR(full_date), '-', QUARTER(full_date)) AS calendar_year_quarter,
        CONCAT(YEAR(full_date), '-', LPAD(MONTH(full_date), 2, '0')) AS calendar_year_month,
        CASE
           WHEN MONTH(full_date) <= 6 THEN 1
           ELSE 2
        END AS half_year,
        CASE
           WHEN MONTH(full_date) <= 6 THEN 'H1'
           ELSE 'H2'
        END AS half_year_name,
        --TODO: add ... as iso_week, --check out: https://docs.microsoft.com/en-us/sql/t-sql/functions/datepart-transact-sql?view=sql-server-ver15#arguments (iso_week item)
        CONCAT_WS('-',
            YEAR(DATE_ADD(full_date, 4 - (DAYOFWEEK(full_date) + 5) % 7 + 1)),
            LPAD(WEEKOFYEAR(full_date), 3, "W0"),
            (DAYOFWEEK(full_date) + 5) % 7 + 1
        ) AS iso_week_name
    FROM date_sequence
)
SELECT * FROM date_dimension