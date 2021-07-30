ID_GDELT_HEADERS = [
    # Non-sentiment information about article
    'gkg_id', 'date', 'source', 'source_name', 'doc_id',
    'themes', 'locations', 'persons', 'orgs'
]

NONID_GDELT_HEADERS = [
    # "Core emotional dimensions" & wc - see 1.5TONE in GKG codebook for details
    'tone',  # pos - neg
    'pos',  # % of words with "positive emotional connotation"
    'neg',  # % of words with "negative emotional connotation"
    'polarity',  # how emotionally charged is the text (as a %)
    'ard',  # activity reference density
    'srd',  # self/group reference density
    'wc',  # word count
    # GCAM entries
    # Lexicoder sentiment dictionary
    'lexicode_neg', 'lexicode_pos',
    # Lexicoder Topic Dictionaries
    'MACROECONOMICS', 'ENERGY', 'FISHERIES', 
    'TRANSPORTATION', 'CRIME', 'SOCIAL_WELFARE',
    'HOUSING', 'FINANCE', 'DEFENCE', 'SSTC',
    'FOREIGN_TRADE', 'CIVIL_RIGHTS', 
    'INTL_AFFAIRS', 'GOVERNMENT_OPS',
    'LAND-WATER-MANAGEMENT', 'CULTURE',
    'PROV_LOCAL', 'INTERGOVERNMENTAL',
    'CONSTITUTIONAL_NATL_UNITY', 'ABORIGINAL',
    'RELIGION', 'HEALTHCARE', 'AGRICULTURE',
    'FORESTRY', 'LABOUR', 'IMMIGRATION',
    'EDUCATION', 'ENVIRONMENT',
    # Central Bank Financial Stability Sentiment
    'finstab_pos', 'finstab_neg', 'finstab_neutral',
    # Loughran & McDonald Financial Sentiment
    'finsent_neg', 'finsent_pos', 'finsent_unc',
    # Opinion observer
    'opin_neg', 'opin_pos',
    # SentiWord
    'sent_pos', 'sent_neg', 'sent_pol'
]

RAW_GDELT_HEADERS = ID_GDELT_HEADERS + NONID_GDELT_HEADERS
