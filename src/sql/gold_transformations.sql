
-- ============================================================
-- ASSET MANAGEMENT PIPELINE
-- GOLD TRANSFORMATIONS
-- Spark SQL compatible
-- ============================================================


-- ------------------------------------------------------------
-- 1. ELIGIBLE TRANSACTIONS
-- ------------------------------------------------------------

CREATE OR REPLACE TEMP VIEW eligible_transactions AS

SELECT
    t.*

FROM transactions t

INNER JOIN security_master s

    ON t.security_id = s.security_id

    AND t.trade_date >= s.effective_from

    AND (
        s.effective_to IS NULL
        OR t.trade_date <= s.effective_to
    );


-- ------------------------------------------------------------
-- 2. TRANSACTION SECURITY QUARANTINE
-- ------------------------------------------------------------

CREATE OR REPLACE TEMP VIEW transaction_security_quarantine AS

SELECT
    t.*,

    CASE
        WHEN s.security_id IS NULL
            THEN 'UNKNOWN_SECURITY'

        ELSE 'INACTIVE_SECURITY'
    END AS rejection_reason

FROM transactions t

LEFT ANTI JOIN eligible_transactions e

    ON t.transaction_id = e.transaction_id

LEFT JOIN security_master s

    ON t.security_id = s.security_id;


-- ------------------------------------------------------------
-- 3. SELECT LATEST ELIGIBLE OPENING POSITION SNAPSHOTS
-- ------------------------------------------------------------

CREATE OR REPLACE TEMP VIEW eligible_positions AS

SELECT
    portfolio_id,
    security_id,
    position_date,
    quantity,
    book_value,
    currency,
    source_file,
    file_checksum,
    run_id

FROM (

    SELECT

        p.*,

        ROW_NUMBER() OVER (

            PARTITION BY
                p.portfolio_id,
                p.security_id

            ORDER BY
                p.position_date DESC

        ) AS snapshot_rank

    FROM positions p

    INNER JOIN security_master s

        ON p.security_id = s.security_id

       AND p.position_date >= s.effective_from

       AND (
            s.effective_to IS NULL
            OR p.position_date <= s.effective_to
       )

    WHERE p.position_date <= DATE('${valuation_date}')

) ranked

WHERE snapshot_rank = 1;


-- ------------------------------------------------------------
-- 4. TRANSACTION MOVEMENTS
-- ------------------------------------------------------------

CREATE OR REPLACE TEMP VIEW transaction_movements AS

SELECT

    p.portfolio_id,

    p.security_id,

    SUM(t.quantity) AS net_transaction_quantity,

    COUNT(DISTINCT t.transaction_id) AS transaction_count

FROM eligible_positions p

INNER JOIN eligible_transactions t

    ON p.portfolio_id = t.portfolio_id

    AND p.security_id = t.security_id

    AND t.trade_date > p.position_date

    AND t.trade_date <= DATE('${valuation_date}')

GROUP BY

    p.portfolio_id,

    p.security_id;


-- ------------------------------------------------------------
-- 5. CLOSING POSITIONS
-- ------------------------------------------------------------

CREATE OR REPLACE TEMP VIEW closing_positions AS

SELECT

    p.portfolio_id,

    p.security_id,

    p.position_date,

    DATE('${valuation_date}') AS valuation_date,

    p.currency,

    p.quantity AS opening_quantity,

    COALESCE(
        t.net_transaction_quantity,
        0.0
    ) AS net_transaction_quantity,

    COALESCE(
        t.transaction_count,
        0
    ) AS transaction_count,

    p.quantity +

    COALESCE(
        t.net_transaction_quantity,
        0.0
    ) AS closing_quantity,

    p.book_value AS opening_book_value,

    p.source_file,

    p.file_checksum,

    p.run_id

FROM eligible_positions p

LEFT JOIN transaction_movements t

    ON p.portfolio_id = t.portfolio_id

    AND p.security_id = t.security_id;


-- ------------------------------------------------------------
-- 6. MARKET PRICE CANDIDATES
-- ------------------------------------------------------------

CREATE OR REPLACE TEMP VIEW position_pricing AS

SELECT

    p.*,

    m.price,

    m.price_date,

    m.price_source,

    m.price_currency

FROM closing_positions p

LEFT JOIN market_prices m

    ON p.security_id = m.security_id

    AND m.price_date <= DATE('${valuation_date}');


-- ------------------------------------------------------------
-- 7. LATEST MARKET PRICE
-- ------------------------------------------------------------

CREATE OR REPLACE TEMP VIEW latest_position_pricing AS

SELECT *

FROM (

    SELECT

        p.*,

        ROW_NUMBER() OVER (

            PARTITION BY
                portfolio_id,
                security_id,
                position_date

            ORDER BY
                price_date DESC NULLS LAST,
                price_source ASC NULLS LAST

        ) AS price_rank

    FROM position_pricing p

) ranked

WHERE price_rank = 1;


-- ------------------------------------------------------------
-- 8. PRICE VALIDATION
-- ------------------------------------------------------------

CREATE OR REPLACE TEMP VIEW priced_positions AS

SELECT

    *,

    CASE

        WHEN price IS NULL
            THEN 'MISSING_MARKET_PRICE'

        WHEN price <= 0
            THEN 'INVALID_MARKET_PRICE'

        WHEN price_currency IS NULL
            THEN 'MISSING_PRICE_CURRENCY'

        WHEN price_currency <> currency
            THEN 'PRICE_CURRENCY_MISMATCH'

        WHEN DATEDIFF(
            DATE('${valuation_date}'),
            price_date
        ) > ${max_price_age_days}

            THEN 'STALE_MARKET_PRICE'

        ELSE NULL

    END AS valuation_rejection_reason

FROM latest_position_pricing;


-- ------------------------------------------------------------
-- 9. TRUSTED PORTFOLIO POSITIONS
-- ------------------------------------------------------------

CREATE OR REPLACE TEMP VIEW trusted_positions AS

SELECT

    portfolio_id,

    security_id,

    position_date,

    valuation_date,

    currency,

    opening_quantity,

    net_transaction_quantity,

    transaction_count,

    closing_quantity,

    price,

    price_date,

    price_source,

    price_currency,

    closing_quantity * price AS market_value,

    opening_book_value,

    source_file,

    file_checksum,

    run_id,

    CURRENT_TIMESTAMP() AS published_at

FROM priced_positions

WHERE valuation_rejection_reason IS NULL;


-- ------------------------------------------------------------
-- 10. POSITION QUARANTINE
-- ------------------------------------------------------------

CREATE OR REPLACE TEMP VIEW position_quarantine AS

SELECT

    *,

    valuation_rejection_reason AS rejection_reason

FROM priced_positions

WHERE valuation_rejection_reason IS NOT NULL;


-- ------------------------------------------------------------
-- 11. COMPLIANCE EXCEPTIONS
-- ------------------------------------------------------------

CREATE OR REPLACE TEMP VIEW compliance_exceptions AS

SELECT DISTINCT

    t.transaction_id,

    t.portfolio_id,

    t.security_id,

    t.trade_date,

    t.quantity,

    t.price,

    t.currency,

    r.restriction_type,

    r.effective_from AS restriction_effective_from,

    r.effective_to AS restriction_effective_to,

    r.status,

    t.source_file,

    t.file_checksum,

    t.run_id,

    'TRADE_DURING_ACTIVE_RESTRICTION'
        AS exception_type,

    CURRENT_TIMESTAMP() AS detected_at

FROM eligible_transactions t

INNER JOIN restricted_securities r

    ON t.security_id = r.security_id

    AND t.trade_date >= r.effective_from

    AND (
        r.effective_to IS NULL
        OR t.trade_date <= r.effective_to
    )

    AND UPPER(TRIM(r.status)) = 'ACTIVE';


-- ------------------------------------------------------------
-- 12. PORTFOLIO RECONCILIATION
-- ------------------------------------------------------------

CREATE OR REPLACE TEMP VIEW portfolio_reconciliation AS

SELECT

    currency,

    COUNT(*) AS position_count,

    SUM(closing_quantity) AS total_closing_quantity,

    SUM(market_value) AS total_market_value,

    SUM(opening_book_value) AS total_opening_book_value

FROM trusted_positions

GROUP BY currency;


-- ============================================================
-- END GOLD TRANSFORMATIONS
-- ============================================================
