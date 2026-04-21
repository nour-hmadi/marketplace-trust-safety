-- ============================================================
-- MARKETPLACE TRUST & SAFETY - INVESTIGATION QUERIES
-- ============================================================


-- ============================================================
-- 1. GET ALL HIGH RISK SELLERS
-- ============================================================
SELECT
    seller_id,
    seller_name,
    region,
    verification_status,
    total_orders,
    total_returns,
    return_rate,
    total_complaints,
    high_severity_complaints,
    risk_score,
    risk_label
FROM curated.seller_risk
WHERE risk_label = 'HIGH'
ORDER BY risk_score DESC;


-- ============================================================
-- 2. GET SELLERS WITH SUSPICIOUS RETURN RATES (above 40%)
-- ============================================================
SELECT
    seller_id,
    seller_name,
    total_orders,
    total_returns,
    ROUND(return_rate * 100, 2) AS return_rate_pct,
    risk_score
FROM curated.seller_risk
WHERE return_rate > 0.4
ORDER BY return_rate DESC;


-- ============================================================
-- 3. GET SELLERS WITH MULTIPLE HIGH SEVERITY COMPLAINTS
-- ============================================================
SELECT
    seller_id,
    seller_name,
    region,
    total_complaints,
    high_severity_complaints,
    risk_label
FROM curated.seller_risk
WHERE high_severity_complaints >= 2
ORDER BY high_severity_complaints DESC;


-- ============================================================
-- 4. GET ALL FRAUD PATTERNS DETECTED
-- ============================================================
SELECT
    seller_id,
    pattern_type,
    indicator_value,
    threshold,
    flagged_at,
    explanation
FROM curated.fraud_patterns
ORDER BY flagged_at DESC;


-- ============================================================
-- 5. UNVERIFIED SELLERS WITH HIGH RISK SCORE
-- ============================================================
SELECT
    seller_id,
    seller_name,
    region,
    verification_status,
    risk_score,
    risk_label,
    total_complaints
FROM curated.seller_risk
WHERE verification_status = 'unverified'
AND risk_label IN ('HIGH', 'MEDIUM')
ORDER BY risk_score DESC;


-- ============================================================
-- 6. COMPLAINT BREAKDOWN BY CATEGORY PER SELLER
-- ============================================================
SELECT
    seller_id,
    category,
    COUNT(*) AS complaint_count,
    SUM(CASE WHEN severity = 'high' THEN 1 ELSE 0 END) AS high_severity_count
FROM refined.complaints
GROUP BY seller_id, category
ORDER BY complaint_count DESC;


-- ============================================================
-- 7. DAILY ORDER VOLUME PER SELLER (detect spikes)
-- ============================================================
SELECT
    seller_id,
    DATE(event_ts) AS order_date,
    COUNT(order_id) AS daily_orders,
    SUM(amount) AS daily_revenue
FROM refined.orders
GROUP BY seller_id, DATE(event_ts)
ORDER BY daily_orders DESC;


-- ============================================================
-- 8. SELLERS WITH BOTH HIGH RETURNS AND HIGH COMPLAINTS
-- ============================================================
SELECT
    seller_id,
    seller_name,
    return_rate,
    total_complaints,
    high_severity_complaints,
    risk_score,
    risk_label
FROM curated.seller_risk
WHERE return_rate > 0.3
AND total_complaints >= 2
ORDER BY risk_score DESC;


-- ============================================================
-- 9. FULL INVESTIGATION PROFILE FOR A SPECIFIC SELLER
-- ============================================================
SELECT
    r.seller_id,
    r.seller_name,
    r.region,
    r.verification_status,
    r.total_orders,
    r.total_returns,
    r.return_rate,
    r.total_complaints,
    r.high_severity_complaints,
    r.avg_order_amount,
    r.risk_score,
    r.risk_label,
    f.pattern_type,
    f.explanation
FROM curated.seller_risk r
LEFT JOIN curated.fraud_patterns f
    ON r.seller_id = f.seller_id
WHERE r.seller_id = 'S55';


-- ============================================================
-- 10. TOP 10 SELLERS TO INVESTIGATE TODAY
-- ============================================================
SELECT
    seller_id,
    seller_name,
    region,
    risk_score,
    risk_label,
    return_rate,
    high_severity_complaints
FROM curated.seller_risk
ORDER BY risk_score DESC
LIMIT 10;