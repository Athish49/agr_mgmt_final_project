-- Overall row count
SELECT COUNT(*) AS total_rows
FROM `fa25-i535-agr-chronicdisease.cdi_lake.cdi_clean`;

-- Year coverage
SELECT
  YearStart,
  COUNT(*) AS n_rows
FROM `fa25-i535-agr-chronicdisease.cdi_lake.cdi_clean`
GROUP BY YearStart
ORDER BY YearStart;

-- Rows per state
SELECT
  LocationAbbr,
  COUNT(*) AS n_rows
FROM `fa25-i535-agr-chronicdisease.cdi_lake.cdi_clean`
GROUP BY LocationAbbr
ORDER BY n_rows DESC;

-- Distribution of indicators by topic
SELECT
  Topic,
  COUNT(*) AS indicators_count
FROM `fa25-i535-agr-chronicdisease.cdi_lake.cdi_clean`
GROUP BY Topic
ORDER BY indicators_count DESC;

-- Average DataValue by topic
SELECT
  Topic,
  AVG(DataValue) AS avg_value,
  MIN(DataValue) AS min_value,
  MAX(DataValue) AS max_value,
  COUNT(*) AS n_rows
FROM `fa25-i535-agr-chronicdisease.cdi_lake.cdi_clean`
WHERE DataValue IS NOT NULL
  AND DataValueUnit = '%'
GROUP BY Topic
ORDER BY avg_value DESC;

-- Trend of average DataValue over time for a topic in one state
SELECT
  YearStart,
  AVG(DataValue) AS avg_value,
  COUNT(*) AS n_rows
FROM `fa25-i535-agr-chronicdisease.cdi_lake.cdi_clean`
WHERE LocationAbbr = 'CA'
  AND Topic = 'Diabetes'
  AND DataValue IS NOT NULL
  AND DataValueUnit = '%'
GROUP BY YearStart
ORDER BY YearStart;

-- Average DataValue by sex for one topic + state
SELECT
  YearStart,
  Stratification1 AS sex,
  AVG(DataValue) AS avg_value,
  COUNT(*) AS n_rows
FROM `fa25-i535-agr-chronicdisease.cdi_lake.cdi_clean`
WHERE LocationAbbr = 'CA'
  AND Topic = 'Diabetes'
  AND StratificationCategory1 = 'Sex'
  AND DataValue IS NOT NULL
  AND DataValueUnit = '%'
GROUP BY YearStart, sex
ORDER BY YearStart, sex;

-- Check how many rows have both low & high confidence limits
SELECT
  COUNT(*) AS total_rows,
  SUM(CASE WHEN LowConfidenceLimit IS NOT NULL 
            AND HighConfidenceLimit IS NOT NULL 
           THEN 1 ELSE 0 END) AS rows_with_ci
FROM `fa25-i535-agr-chronicdisease.cdi_lake.cdi_clean`;

-- Gap between DataValue and CI 
SELECT
  Topic,
  AVG(HighConfidenceLimit - LowConfidenceLimit) AS avg_ci_width,
  COUNT(*) AS n_rows
FROM `fa25-i535-agr-chronicdisease.cdi_lake.cdi_clean`
WHERE LowConfidenceLimit IS NOT NULL
  AND HighConfidenceLimit IS NOT NULL
  AND DataValueUnit = '%'
GROUP BY Topic
ORDER BY avg_ci_width DESC;