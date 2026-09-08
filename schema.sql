-- ============================================================================
-- Harvard Art Museums Artifacts — MySQL Schema
-- Run this once to create the database and the 3 tables used by both the
-- Jupyter notebook (harvard_artifacts_project_mysql_fixed.ipynb) and the
-- Streamlit app (app.py). The app also creates these automatically the first
-- time you click "Insert into SQL", so running this file by hand is optional.
-- ============================================================================

CREATE DATABASE IF NOT EXISTS `harvard_artifacts` CHARACTER SET utf8mb4;
USE `harvard_artifacts`;

-- ----------------------------------------------------------------------------
-- artifact_metadata: one row per artifact (the "parent" table)
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS artifact_metadata (
    id              INT PRIMARY KEY,
    title           TEXT,
    culture         VARCHAR(255),
    period          VARCHAR(255),
    century         VARCHAR(255),
    medium          TEXT,
    dimensions      VARCHAR(255),
    description     TEXT,
    department      VARCHAR(255),
    classification  VARCHAR(255),
    accessionyear   INT,
    accessionmethod VARCHAR(255)
);

-- ----------------------------------------------------------------------------
-- artifact_media: one row per artifact, media/ranking stats
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS artifact_media (
    objectid   INT PRIMARY KEY,
    imagecount INT,
    mediacount INT,
    colorcount INT,
    `rank`     INT,
    datebegin  INT,
    dateend    INT,
    FOREIGN KEY (objectid) REFERENCES artifact_metadata(id)
);

-- ----------------------------------------------------------------------------
-- artifact_colors: many rows per artifact (one per detected color)
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS artifact_colors (
    color_id  INT AUTO_INCREMENT PRIMARY KEY,
    objectid  INT,
    color     VARCHAR(50),
    spectrum  VARCHAR(50),
    hue       VARCHAR(50),
    percent   FLOAT,
    css3      VARCHAR(50),
    FOREIGN KEY (objectid) REFERENCES artifact_metadata(id)
);

-- ============================================================================
-- 26 analysis queries (20 required + 6 bonus), also wired into the
-- Streamlit app's "Query & Visualization" dropdown. Kept here as a
-- ready-to-run SQL workspace, per the project spec.
-- ============================================================================

-- 🏺 artifact_metadata queries
-- Q1. Artifacts from the 11th century, Byzantine culture
SELECT * FROM artifact_metadata
WHERE century LIKE '%11th%' AND culture = 'Byzantine';

-- Q2. Unique cultures represented
SELECT DISTINCT culture FROM artifact_metadata
WHERE culture IS NOT NULL ORDER BY culture;

-- Q3. Artifacts from the Archaic Period
SELECT * FROM artifact_metadata
WHERE period LIKE '%Archaic%';

-- Q4. Artifact titles ordered by accession year (desc)
SELECT title, accessionyear FROM artifact_metadata
WHERE accessionyear IS NOT NULL
ORDER BY accessionyear DESC;

-- Q5. Artifact count per department
SELECT department, COUNT(*) AS artifact_count
FROM artifact_metadata
GROUP BY department
ORDER BY artifact_count DESC;

-- 🖼️ artifact_media queries
-- Q6. Artifacts with more than 1 image
SELECT md.id, md.title, m.imagecount
FROM artifact_media m
JOIN artifact_metadata md ON md.id = m.objectid
WHERE m.imagecount > 1;

-- Q7. Average rank of all artifacts
SELECT AVG(`rank`) AS average_rank FROM artifact_media;

-- Q8. Artifacts with colorcount > mediacount
SELECT md.id, md.title, m.colorcount, m.mediacount
FROM artifact_media m
JOIN artifact_metadata md ON md.id = m.objectid
WHERE m.colorcount > m.mediacount;

-- Q9. Artifacts created between 1500 and 1600
SELECT md.id, md.title, m.datebegin, m.dateend
FROM artifact_media m
JOIN artifact_metadata md ON md.id = m.objectid
WHERE m.datebegin >= 1500 AND m.dateend <= 1600;

-- Q10. Artifacts with no media files
SELECT COUNT(*) AS no_media_count
FROM artifact_media
WHERE mediacount IS NULL OR mediacount = 0;

-- 🎨 artifact_colors queries
-- Q11. Distinct hues in the dataset
SELECT DISTINCT hue FROM artifact_colors
WHERE hue IS NOT NULL ORDER BY hue;

-- Q12. Top 5 most used colors by frequency
SELECT color, COUNT(*) AS frequency
FROM artifact_colors
WHERE color IS NOT NULL
GROUP BY color
ORDER BY frequency DESC
LIMIT 5;

-- Q13. Average coverage percentage per hue
SELECT hue, AVG(percent) AS avg_percent
FROM artifact_colors
WHERE hue IS NOT NULL
GROUP BY hue
ORDER BY avg_percent DESC;

-- Q14. Colors used for a given artifact ID (replace 1 with a real id)
SELECT * FROM artifact_colors WHERE objectid = 1;

-- Q15. Total number of color entries
SELECT COUNT(*) AS total_color_entries FROM artifact_colors;

-- 🔗 Join-based queries
-- Q16. Titles and hues for Byzantine culture artifacts
SELECT md.title, c.hue
FROM artifact_metadata md
JOIN artifact_colors c ON c.objectid = md.id
WHERE md.culture = 'Byzantine';

-- Q17. Each artifact title with its associated hues
SELECT md.title, GROUP_CONCAT(DISTINCT c.hue) AS hues
FROM artifact_metadata md
JOIN artifact_colors c ON c.objectid = md.id
GROUP BY md.title;

-- Q18. Titles, cultures, media ranks where period is not null
SELECT md.title, md.culture, m.`rank`
FROM artifact_metadata md
JOIN artifact_media m ON m.objectid = md.id
WHERE md.period IS NOT NULL;

-- Q19. Top-10-ranked artifacts that include hue 'Grey'
SELECT DISTINCT md.title, m.`rank`
FROM artifact_metadata md
JOIN artifact_media m ON m.objectid = md.id
JOIN artifact_colors c ON c.objectid = md.id
WHERE c.hue = 'Grey'
ORDER BY m.`rank` ASC
LIMIT 10;

-- Q20. Artifact count & avg media count per classification
SELECT md.classification,
       COUNT(DISTINCT md.id) AS artifact_count,
       AVG(m.mediacount) AS avg_mediacount
FROM artifact_metadata md
JOIN artifact_media m ON m.objectid = md.id
GROUP BY md.classification
ORDER BY artifact_count DESC;

-- ⭐ Bonus / custom queries
-- B1. Top 10 mediums by artifact count
SELECT medium, COUNT(*) AS artifact_count
FROM artifact_metadata
WHERE medium IS NOT NULL
GROUP BY medium
ORDER BY artifact_count DESC
LIMIT 10;

-- B2. Artifacts per century
SELECT century, COUNT(*) AS artifact_count
FROM artifact_metadata
WHERE century IS NOT NULL
GROUP BY century
ORDER BY artifact_count DESC;

-- B3. Top 10 accession methods used
SELECT accessionmethod, COUNT(*) AS artifact_count
FROM artifact_metadata
WHERE accessionmethod IS NOT NULL
GROUP BY accessionmethod
ORDER BY artifact_count DESC
LIMIT 10;

-- B4. Average image count per department
SELECT md.department, AVG(m.imagecount) AS avg_imagecount
FROM artifact_metadata md
JOIN artifact_media m ON m.objectid = md.id
GROUP BY md.department
ORDER BY avg_imagecount DESC;

-- B5. Artifacts with the widest date range (dateend - datebegin)
SELECT md.title, m.datebegin, m.dateend,
       (m.dateend - m.datebegin) AS span_years
FROM artifact_metadata md
JOIN artifact_media m ON m.objectid = md.id
WHERE m.datebegin IS NOT NULL AND m.dateend IS NOT NULL
ORDER BY span_years DESC
LIMIT 10;

-- B6. Most common CSS3 color overall
SELECT css3, COUNT(*) AS frequency
FROM artifact_colors
WHERE css3 IS NOT NULL
GROUP BY css3
ORDER BY frequency DESC
LIMIT 10;
