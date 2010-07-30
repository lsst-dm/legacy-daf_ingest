-- SQL script that updates Raw_Amp_Exposure_Metadata and Science_Ccd_Exposure_Metadata
-- tables such that metadata keys with inconsistent type have consitent type.
--
-- This has been observed to happen in the following cases:
--   * When a FITS header card that nominally has floating point values happens
--     to have an integer value (i.e. was printed without a decimal point)
--   * When a FITS header card that nominally has a numeric value is set to
--     a string value to indicate a missing value (e.g. "Null")
--

-- Cleaner but slower:
--
-- UPDATE Raw_Amp_Exposure_Metadata
-- SET doubleValue = intValue, intValue = NULL
-- WHERE intValue IS NOT NULL AND metadataKey IN (
--    SELECT DISTINCT metadataKey FROM Raw_Amp_Exposure_Metadata
--    WHERE doubleValue IS NOT NULL AND metadataKey IN (
--        SELECT DISTINCT metadataKey FROM Raw_Amp_Exposure_Metadata
--        WHERE intValue IS NOT NULL
--    )
-- );
--
-- UPDATE Raw_Amp_Exposure_Metadata
-- SET stringValue = NULL
-- WHERE stringValue RLIKE '[[:blank:]]*[Nn][Uu][Ll]{2}[[:blank:]]*' AND metadataKey IN (
--     SELECT DISTINCT metadataKey FROM Raw_Amp_Exposure_Metadata
--     WHERE (doubleValue IS NOT NULL OR intValue IS NOT NULL) AND metadataKey IN (
--         SELECT DISTINCT metadataKey FROM Raw_Amp_Exposure_Metadata
--         WHERE stringValue IS NOT NULL
--     )
-- );
--
-- (and the same queries on Science_Ccd_Exposure_Metadata)

CREATE TABLE _Keys1 (metadataKey VARCHAR(255) NOT NULL PRIMARY KEY) ENGINE=MEMORY;
CREATE TABLE _Keys2 (metadataKey VARCHAR(255) NOT NULL PRIMARY KEY) ENGINE=MEMORY;

INSERT INTO _Keys1
    SELECT DISTINCT metadataKey
    FROM Raw_Amp_Exposure_Metadata WHERE intValue IS NOT NULL;

INSERT INTO _Keys2
    SELECT DISTINCT a.metadataKey
    FROM Raw_Amp_Exposure_Metadata AS a INNER JOIN
         _Keys1 AS b ON (a.metadataKey = b.metadataKey)
    WHERE a.doubleValue IS NOT NULL;

UPDATE Raw_Amp_Exposure_Metadata AS a INNER JOIN
       _Keys2 AS b ON (a.metadataKey = b.metadataKey)
SET a.doubleValue = a.intValue,
    a.intValue    = NULL
WHERE a.intValue IS NOT NULL;

TRUNCATE TABLE _Keys1;
TRUNCATE TABLE _Keys2;

-- --------

INSERT INTO _Keys1
    SELECT DISTINCT metadataKey
    FROM Raw_Amp_Exposure_Metadata WHERE stringValue IS NOT NULL;

INSERT INTO _Keys2
    SELECT DISTINCT a.metadataKey
    FROM Raw_Amp_Exposure_Metadata AS a INNER JOIN
         _Keys1 AS b ON (a.metadataKey = b.metadataKey)
    WHERE a.doubleValue IS NOT NULL OR a.intValue IS NOT NULL;

UPDATE Raw_Amp_Exposure_Metadata AS a INNER JOIN
       _Keys2 AS b ON (a.metadataKey = b.metadataKey)
SET a.stringValue = NULL
WHERE a.stringValue RLIKE '[[:blank:]]*[Nn][Uu][Ll]{2}[[:blank:]]*';

TRUNCATE TABLE _Keys1;
TRUNCATE TABLE _Keys2;

-- --------

INSERT INTO _Keys1
    SELECT DISTINCT metadataKey
    FROM Science_Ccd_Exposure_Metadata WHERE intValue IS NOT NULL;

INSERT INTO _Keys2
    SELECT DISTINCT a.metadataKey
    FROM Science_Ccd_Exposure_Metadata AS a INNER JOIN
         _Keys1 AS b ON (a.metadataKey = b.metadataKey)
    WHERE a.doubleValue IS NOT NULL;

UPDATE Science_Ccd_Exposure_Metadata AS a INNER JOIN
       _Keys2 AS b ON (a.metadataKey = b.metadataKey)
SET a.doubleValue = a.intValue,
    a.intValue    = NULL
WHERE a.intValue IS NOT NULL;

TRUNCATE TABLE _Keys1;
TRUNCATE TABLE _Keys2;

-- --------

INSERT INTO _Keys1
    SELECT DISTINCT metadataKey
    FROM Science_Ccd_Exposure_Metadata WHERE stringValue IS NOT NULL;

INSERT INTO _Keys2
    SELECT DISTINCT a.metadataKey
    FROM Science_Ccd_Exposure_Metadata AS a INNER JOIN
         _Keys1 AS b ON (a.metadataKey = b.metadataKey)
    WHERE a.doubleValue IS NOT NULL OR a.intValue IS NOT NULL;

UPDATE Science_Ccd_Exposure_Metadata AS a INNER JOIN
       _Keys2 AS b ON (a.metadataKey = b.metadataKey)
SET a.stringValue = NULL
WHERE a.stringValue RLIKE '[[:blank:]]*[Nn][Uu][Ll]{2}[[:blank:]]*';

DROP TABLE _Keys1;
DROP TABLE _Keys2;

-- Print out any keys that still have inconsistent type

SELECT 'Raw_Amp_Exposure_Metadata keys with duplicate types:';

SELECT t.metadataKey, count(*) AS n, GROUP_CONCAT(t.type SEPARATOR ',') AS types
FROM (
    SELECT DISTINCT metadataKey, IF(stringValue IS NOT NULL, "string", IF(intValue IS NOT NULL, "int", "double")) AS type
    FROM Raw_Amp_Exposure_Metadata
    WHERE stringValue IS NOT NULL OR intValue IS NOT NULL OR doubleValue IS NOT NULL
) AS t
GROUP BY t.metadataKey
HAVING n > 1;

SELECT 'Science_Ccd_Exposure_Metadata keys with duplicate types:';

SELECT t.metadataKey, count(*) AS n, GROUP_CONCAT(t.type SEPARATOR ',') AS types
FROM (
    SELECT DISTINCT metadataKey, IF(stringValue IS NOT NULL, "string", IF(intValue IS NOT NULL, "int", "double")) AS type
    FROM Science_Ccd_Exposure_Metadata
    WHERE stringValue IS NOT NULL OR intValue IS NOT NULL OR doubleValue IS NOT NULL
) AS t
GROUP BY t.metadataKey
HAVING n > 1;

