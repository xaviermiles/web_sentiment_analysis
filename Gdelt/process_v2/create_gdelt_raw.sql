DROP TABLE IF EXISTS gdelt_raw;
DROP TYPE IF EXISTS location_item;
DROP TYPE IF EXISTS countries_item;

CREATE TYPE location_item AS (
    type         INTEGER,
    full_name    TEXT,
    country_code TEXT,
    ADM1_code    TEXT,
    lat          NUMERIC,
    long         NUMERIC,
    feature_id   TEXT
);

CREATE TYPE countries_item AS (
    NZ    BOOLEAN,
    AU    BOOLEAN,
    CA    BOOLEAN,
    UK    BOOLEAN
);

CREATE TABLE gdelt_raw (
    -- Non-sentiment information about article
    gkg_id                    VARCHAR(25) PRIMARY KEY,
    datetime                  TIMESTAMP   NOT NULL,
    source                    INTEGER     NOT NULL,
    source_name               TEXT        NOT NULL,
    doc_id                    TEXT        NOT NULL,
    themes                    TEXT[],
    locations                 location_item[],
    persons                   TEXT[],
    orgs                      TEXT[],
    countries                 countries_item,
    -- "Core emotional dimensions" & wc - see 1.5TONE in GKG codebook for details
    tone                      NUMERIC,
    pos                       NUMERIC,
    neg                       NUMERIC,
    polarity                  NUMERIC,
    ard                       NUMERIC,
    srd                       NUMERIC,
    wc                        INTEGER,
    -- GCAM entries - see V2GCAM in GKG codebook for details
    -- NB: INTEGER are count dimensions, REAL are value dimensions
    -- Lexicoder sentiment dictionary
    lexicode_neg              INTEGER,
    lexicode_pos              INTEGER,
    -- lexicoder Topic Dictionaries
    macroeconomics            INTEGER,
    energy                    INTEGER,
    fisheries                 INTEGER,
    transportation            INTEGER,
    crime                     INTEGER,
    social_welfare            INTEGER,
    housing                   INTEGER,
    finance                   INTEGER,
    defence                   INTEGER,
    sstc                      INTEGER,
    foreign_trade             INTEGER,
    civil_rights              INTEGER,
    intl_rights               INTEGER,
    govt_ops                  INTEGER,
    land_water_management     INTEGER,
    culture                   INTEGER,
    prov_local                INTEGER,
    intergovernmental         INTEGER,
    constitutional_natl_unity INTEGER,
    aboriginal                INTEGER,
    religion                  INTEGER,
    healthcare                INTEGER,
    agriculture               INTEGER,
    forestry                  INTEGER,
    labour                    INTEGER,
    immigration               INTEGER,
    education                 INTEGER,
    environment               INTEGER,
    -- Central Bank Financial Stability Sentiment
    finstab_pos               INTEGER,
    finstab_neg               INTEGER,
    finstab_neutral           INTEGER,
    -- Loughran & McDonald Financial Sentiment
    finsent_neg               INTEGER,
    finsent_pos               INTEGER,
    finsent_unc               INTEGER,  -- uncertainty
    -- Opinion observer
    opin_neg                  INTEGER,
    opin_pos                  INTEGER,
    -- SentiWord
    sent_pos                  NUMERIC,
    sent_neg                  NUMERIC,
    sent_pol                  NUMERIC
);
