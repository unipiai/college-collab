import csv
import sqlite3
from sqlalchemy import create_engine, text

def delete_db(db_name="schools.db"):
    """Deletes the existing database file if it exists."""
    import os
    if os.path.exists(db_name):
        os.remove(db_name)

def create_database_and_tables(db_name="schools.db"):
    """
    Creates a SQLite database and the necessary tables with the defined schema.
    Also populates the schema_information table.
    """
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()

    # SQL commands to create tables
    table_creation_queries = [
        """
        CREATE TABLE IF NOT EXISTS school_main (
            UNITID BIGINT NOT NULL PRIMARY KEY, OPEID VARCHAR NOT NULL, OPEID6 VARCHAR NOT NULL,
            INSTNM VARCHAR NOT NULL, CITY VARCHAR NOT NULL, STABBR VARCHAR NOT NULL, ZIP VARCHAR NOT NULL,
            ADDR VARCHAR NOT NULL, ACCREDAGENCY VARCHAR NOT NULL, ACCREDCODE VARCHAR NOT NULL,
            INSTURL VARCHAR NOT NULL, NPCURL VARCHAR NOT NULL, MAIN BIGINT NOT NULL, NUMBRANCH BIGINT NOT NULL,
            CONTROL BIGINT NOT NULL, ST_FIPS BIGINT NOT NULL, REGION BIGINT NOT NULL, LOCALE BIGINT NOT NULL,
            LOCALE2 VARCHAR NOT NULL, LATITUDE DOUBLE NOT NULL, LONGITUDE DOUBLE NOT NULL,
            CURROPER BIGINT NOT NULL, OPENADMP BIGINT NOT NULL, T4APPROVALDATE VARCHAR NOT NULL
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS school_characteristics (
            UNITID BIGINT NOT NULL PRIMARY KEY, SCH_DEG VARCHAR NOT NULL, HCM2 BIGINT NOT NULL,
            PREDDEG BIGINT NOT NULL, HIGHDEG BIGINT NOT NULL, CCBASIC BIGINT NOT NULL,
            CCUGPROF BIGINT NOT NULL, CCSIZSET BIGINT NOT NULL, HBCU BIGINT NOT NULL, PBI BIGINT NOT NULL,
            ANNHI BIGINT NOT NULL, TRIBAL BIGINT NOT NULL, AANAPII BIGINT NOT NULL, HSI BIGINT NOT NULL,
            NANTI BIGINT NOT NULL, MENONLY BIGINT NOT NULL, WOMENONLY BIGINT NOT NULL,
            RELAFFIL VARCHAR NOT NULL, DISTANCEONLY BIGINT NOT NULL, SCHTYPE BIGINT NOT NULL,
            OPEFLAG BIGINT NOT NULL, DOLPROVIDER BIGINT NOT NULL, SCORECARD_SECTOR BIGINT NOT NULL,
            FOREIGN KEY (UNITID) REFERENCES school_main(UNITID)
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS school_admissions (
            UNITID BIGINT NOT NULL PRIMARY KEY, ADM_RATE VARCHAR NOT NULL, ADM_RATE_ALL VARCHAR NOT NULL,
            ADM_RATE_SUPP VARCHAR NOT NULL, SATVR25 VARCHAR NOT NULL, SATVR75 VARCHAR NOT NULL,
            SATMT25 VARCHAR NOT NULL, SATMT75 VARCHAR NOT NULL, SATWR25 VARCHAR NOT NULL,
            SATWR75 VARCHAR NOT NULL, SATVRMID VARCHAR NOT NULL, SATMTMID VARCHAR NOT NULL,
            SATWRMID VARCHAR NOT NULL, ACTCM25 VARCHAR NOT NULL, ACTCM75 VARCHAR NOT NULL,
            ACTEN25 VARCHAR NOT NULL, ACTEN75 VARCHAR NOT NULL, ACTMT25 VARCHAR NOT NULL,
            ACTMT75 VARCHAR NOT NULL, ACTWR25 VARCHAR NOT NULL, ACTWR75 VARCHAR NOT NULL,
            ACTCMMID VARCHAR NOT NULL, ACTENMID VARCHAR NOT NULL, ACTMTMID VARCHAR NOT NULL,
            ACTWRMID VARCHAR NOT NULL, SAT_AVG VARCHAR NOT NULL, SAT_AVG_ALL VARCHAR NOT NULL, ADMCON7 VARCHAR NOT NULL,
            FOREIGN KEY (UNITID) REFERENCES school_main(UNITID)
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS school_academics_cip (
            UNITID BIGINT NOT NULL PRIMARY KEY, PCIP01 DOUBLE NOT NULL, PCIP03 DOUBLE NOT NULL, PCIP04 DOUBLE NOT NULL,
            PCIP05 DOUBLE NOT NULL, PCIP09 DOUBLE NOT NULL, PCIP10 DOUBLE NOT NULL, PCIP11 DOUBLE NOT NULL,
            PCIP12 BIGINT NOT NULL, PCIP13 DOUBLE NOT NULL, PCIP14 DOUBLE NOT NULL, PCIP15 DOUBLE NOT NULL,
            PCIP16 DOUBLE NOT NULL, PCIP19 DOUBLE NOT NULL, PCIP22 BIGINT NOT NULL, PCIP23 DOUBLE NOT NULL,
            PCIP24 DOUBLE NOT NULL, PCIP25 BIGINT NOT NULL, PCIP26 DOUBLE NOT NULL, PCIP27 DOUBLE NOT NULL,
            PCIP29 BIGINT NOT NULL, PCIP30 DOUBLE NOT NULL, PCIP31 DOUBLE NOT NULL, PCIP38 DOUBLE NOT NULL,
            PCIP39 DOUBLE NOT NULL, PCIP40 DOUBLE NOT NULL, PCIP41 BIGINT NOT NULL, PCIP42 DOUBLE NOT NULL,
            PCIP43 DOUBLE NOT NULL, PCIP44 DOUBLE NOT NULL, PCIP45 DOUBLE NOT NULL, PCIP46 BIGINT NOT NULL,
            PCIP47 BIGINT NOT NULL, PCIP48 BIGINT NOT NULL, PCIP49 BIGINT NOT NULL, PCIP50 DOUBLE NOT NULL,
            PCIP51 DOUBLE NOT NULL, PCIP52 DOUBLE NOT NULL, PCIP54 DOUBLE NOT NULL, PRGMOFR VARCHAR NOT NULL,
            CIPTITLE1 VARCHAR NOT NULL, CIPTITLE2 VARCHAR NOT NULL, CIPTITLE3 VARCHAR NOT NULL, CIPTITLE4 VARCHAR NOT NULL,
            CIPTITLE5 VARCHAR NOT NULL, CIPTITLE6 VARCHAR NOT NULL,
            FOREIGN KEY (UNITID) REFERENCES school_main(UNITID)
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS school_student_demographics (
            UNITID BIGINT NOT NULL PRIMARY KEY, UGDS BIGINT NOT NULL, UG VARCHAR NOT NULL,
            UGDS_WHITE DOUBLE NOT NULL, UGDS_BLACK DOUBLE NOT NULL, UGDS_HISP DOUBLE NOT NULL,
            UGDS_ASIAN DOUBLE NOT NULL, UGDS_AIAN DOUBLE NOT NULL, UGDS_NHPI DOUBLE NOT NULL,
            UGDS_2MOR DOUBLE NOT NULL, UGDS_NRA DOUBLE NOT NULL, UGDS_UNKN DOUBLE NOT NULL,
            UGDS_MEN DOUBLE NOT NULL, UGDS_WOMEN DOUBLE NOT NULL, UGNONDS VARCHAR NOT NULL,
            GRADS BIGINT NOT NULL, UG12MN BIGINT NOT NULL, G12MN BIGINT NOT NULL, PPTUG_EF DOUBLE NOT NULL,
            UG25ABV DOUBLE NOT NULL,
            FOREIGN KEY (UNITID) REFERENCES school_main(UNITID)
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS school_costs (
            UNITID BIGINT NOT NULL PRIMARY KEY, COSTT4_A VARCHAR NOT NULL, COSTT4_P VARCHAR NOT NULL,
            TUITIONFEE_IN VARCHAR NOT NULL, TUITIONFEE_OUT VARCHAR NOT NULL, TUITIONFEE_PROG VARCHAR NOT NULL,
            TUITFTE BIGINT NOT NULL, INEXPFTE BIGINT NOT NULL, BOOKSUPPLY VARCHAR NOT NULL,
            ROOMBOARD_ON VARCHAR NOT NULL, OTHEREXPENSE_ON VARCHAR NOT NULL, ROOMBOARD_OFF VARCHAR NOT NULL,
            OTHEREXPENSE_OFF VARCHAR NOT NULL, OTHEREXPENSE_FAM VARCHAR NOT NULL,
            FOREIGN KEY (UNITID) REFERENCES school_main(UNITID)
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS school_financial_aid (
            UNITID BIGINT NOT NULL PRIMARY KEY, NPT4_PUB VARCHAR NOT NULL, NPT4_PRIV VARCHAR NOT NULL,
            NPT4_PROG VARCHAR NOT NULL, NPT4_OTHER VARCHAR NOT NULL, NPT41_PUB VARCHAR NOT NULL,
            NPT42_PUB VARCHAR NOT NULL, NPT43_PUB VARCHAR NOT NULL, NPT44_PUB VARCHAR NOT NULL,
            NPT45_PUB VARCHAR NOT NULL, NPT41_PRIV VARCHAR NOT NULL, NPT42_PRIV VARCHAR NOT NULL,
            NPT43_PRIV VARCHAR NOT NULL, NPT44_PRIV VARCHAR NOT NULL, NPT45_PRIV VARCHAR NOT NULL,
            PCTPELL DOUBLE NOT NULL, PCTFLOAN DOUBLE NOT NULL, FTFTPCTPELL VARCHAR NOT NULL,
            FTFTPCTFLOAN VARCHAR NOT NULL,
            FOREIGN KEY (UNITID) REFERENCES school_main(UNITID)
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS school_completion_rates (
            UNITID BIGINT NOT NULL PRIMARY KEY, C150_4 DOUBLE NOT NULL, C150_L4 VARCHAR NOT NULL,
            C150_4_POOLED DOUBLE NOT NULL, C150_L4_POOLED VARCHAR NOT NULL, C200_4 DOUBLE NOT NULL,
            C200_L4 VARCHAR NOT NULL, C100_4 DOUBLE NOT NULL, C100_L4 VARCHAR NOT NULL,
            OMAWDP6_FTFT DOUBLE NOT NULL, OMAWDP8_FTFT DOUBLE NOT NULL, OMAWDP6_PTFT DOUBLE NOT NULL,
            OMAWDP8_PTFT DOUBLE NOT NULL, OMAWDP6_FTNFT DOUBLE NOT NULL, OMAWDP8_FTNFT DOUBLE NOT NULL,
            OMAWDP6_PTNFT DOUBLE NOT NULL, OMAWDP8_PTNFT DOUBLE NOT NULL,
            FOREIGN KEY (UNITID) REFERENCES school_main(UNITID)
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS school_retention_rates (
            UNITID BIGINT NOT NULL PRIMARY KEY, RET_FT4 VARCHAR NOT NULL, RET_FTL4 VARCHAR NOT NULL,
            RET_PT4 VARCHAR NOT NULL, RET_PTL4 VARCHAR NOT NULL,
            FOREIGN KEY (UNITID) REFERENCES school_main(UNITID)
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS school_student_debt (
            UNITID BIGINT NOT NULL PRIMARY KEY, DEBT_MDN VARCHAR NOT NULL, GRAD_DEBT_MDN VARCHAR NOT NULL,
            WDRAW_DEBT_MDN VARCHAR NOT NULL, LO_INC_DEBT_MDN VARCHAR NOT NULL, MD_INC_DEBT_MDN VARCHAR NOT NULL,
            HI_INC_DEBT_MDN VARCHAR NOT NULL, DEP_DEBT_MDN VARCHAR NOT NULL, IND_DEBT_MDN VARCHAR NOT NULL,
            PELL_DEBT_MDN VARCHAR NOT NULL, NOPELL_DEBT_MDN VARCHAR NOT NULL, FEMALE_DEBT_MDN VARCHAR NOT NULL,
            MALE_DEBT_MDN VARCHAR NOT NULL, FIRSTGEN_DEBT_MDN VARCHAR NOT NULL, NOTFIRSTGEN_DEBT_MDN VARCHAR NOT NULL,
            GRAD_DEBT_MDN10YR VARCHAR NOT NULL,
            FOREIGN KEY (UNITID) REFERENCES school_main(UNITID)
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS school_repayment_rates (
            UNITID BIGINT NOT NULL PRIMARY KEY, RPY_1YR_RT VARCHAR NOT NULL, COMPL_RPY_1YR_RT VARCHAR NOT NULL,
            NONCOM_RPY_1YR_RT VARCHAR NOT NULL, RPY_3YR_RT VARCHAR NOT NULL, COMPL_RPY_3YR_RT VARCHAR NOT NULL,
            NONCOM_RPY_3YR_RT VARCHAR NOT NULL, RPY_5YR_RT VARCHAR NOT NULL, COMPL_RPY_5YR_RT VARCHAR NOT NULL,
            NONCOM_RPY_5YR_RT VARCHAR NOT NULL, RPY_7YR_RT VARCHAR NOT NULL, COMPL_RPY_7YR_RT VARCHAR NOT NULL,
            NONCOM_RPY_7YR_RT VARCHAR NOT NULL, CDR2 VARCHAR NOT NULL, CDR3 DOUBLE NOT NULL,
            FOREIGN KEY (UNITID) REFERENCES school_main(UNITID)
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS school_earnings_p6 (
            UNITID BIGINT NOT NULL PRIMARY KEY, COUNT_NWNE_P6 VARCHAR NOT NULL, COUNT_WNE_P6 VARCHAR NOT NULL,
            MN_EARN_WNE_P6 VARCHAR NOT NULL, MD_EARN_WNE_P6 VARCHAR NOT NULL, GT_25K_P6 VARCHAR NOT NULL,
            GT_28K_P6 VARCHAR NOT NULL,
            FOREIGN KEY (UNITID) REFERENCES school_main(UNITID)
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS school_earnings_p8 (
            UNITID BIGINT NOT NULL PRIMARY KEY, COUNT_NWNE_P8 VARCHAR NOT NULL, COUNT_WNE_P8 VARCHAR NOT NULL,
            MN_EARN_WNE_P8 VARCHAR NOT NULL, MD_EARN_WNE_P8 VARCHAR NOT NULL, GT_25K_P8 VARCHAR NOT NULL,
            GT_28K_P8 VARCHAR NOT NULL,
            FOREIGN KEY (UNITID) REFERENCES school_main(UNITID)
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS school_earnings_p10 (
            UNITID BIGINT NOT NULL PRIMARY KEY, COUNT_NWNE_P10 VARCHAR NOT NULL, COUNT_WNE_P10 VARCHAR NOT NULL,
            MN_EARN_WNE_P10 VARCHAR NOT NULL, MD_EARN_WNE_P10 VARCHAR NOT NULL, GT_25K_P10 VARCHAR NOT NULL,
            GT_28K_P10 VARCHAR NOT NULL,
            FOREIGN KEY (UNITID) REFERENCES school_main(UNITID)
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS school_faculty (
            UNITID BIGINT NOT NULL PRIMARY KEY, AVGFACSAL BIGINT NOT NULL, PFTFAC DOUBLE NOT NULL,
            STUFACR BIGINT NOT NULL, IRPS_2MOR DOUBLE NOT NULL, IRPS_AIAN DOUBLE NOT NULL, IRPS_ASIAN DOUBLE NOT NULL,
            IRPS_BLACK DOUBLE NOT NULL, IRPS_HISP DOUBLE NOT NULL, IRPS_NHPI DOUBLE NOT NULL, IRPS_NRA DOUBLE NOT NULL,
            IRPS_UNKN DOUBLE NOT NULL, IRPS_WHITE DOUBLE NOT NULL, IRPS_WOMEN DOUBLE NOT NULL, IRPS_MEN DOUBLE NOT NULL,
            FOREIGN KEY (UNITID) REFERENCES school_main(UNITID)
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS schema_information (
            TABLE_NAME VARCHAR NOT NULL, COLUMN_NAME VARCHAR NOT NULL, DATA_TYPE VARCHAR NOT NULL,
            DESCRIPTION TEXT NOT NULL, GROUP_CATEGORY VARCHAR NOT NULL
        );
        """
    ]

    for query in table_creation_queries:
        cursor.execute(query)

    # Clear existing schema information to prevent duplicates on re-run
    cursor.execute("DELETE FROM schema_information;")

    # Populate the schema_information table
    schema_info_inserts = [
        ('school_main', 'UNITID', 'BIGINT', 'Unit ID for institution', 'School Identification'),
        ('school_main', 'OPEID6', 'VARCHAR', '6-digit OPE ID for institution', 'School Identification'),
        ('school_main', 'INSTNM', 'VARCHAR', 'Institution name', 'School Identification'),
        ('school_main', 'OPEID', 'VARCHAR', '8-digit OPE ID for institution', 'School Identification'),
        ('school_main', 'CITY', 'VARCHAR', 'City', 'Location'),
        ('school_main', 'STABBR', 'VARCHAR', 'State postcode', 'Location'),
        ('school_main', 'ZIP', 'VARCHAR', 'ZIP code', 'Location'),
        ('school_main', 'ADDR', 'VARCHAR', 'Address of institution', 'Location'),
        ('school_main', 'ACCREDAGENCY', 'VARCHAR', 'Accreditor for institution', 'Institutional Characteristics'),
        ('school_main', 'ACCREDCODE', 'VARCHAR', 'Code corresponding to accreditor (as captured from PEPS)', 'Institutional Characteristics'),
        ('school_main', 'INSTURL', 'VARCHAR', 'URL for institution''s homepage', 'School Identification'),
        ('school_main', 'NPCURL', 'VARCHAR', 'URL for institution''s net price calculator', 'Costs and Financial Aid'),
        ('school_main', 'MAIN', 'BIGINT', 'Flag for main campus', 'Institutional Characteristics'),
        ('school_main', 'NUMBRANCH', 'BIGINT', 'Number of branch campuses', 'Institutional Characteristics'),
        ('school_main', 'CONTROL', 'BIGINT', 'Control of institution (IPEDS)', 'Institutional Characteristics'),
        ('school_main', 'ST_FIPS', 'BIGINT', 'FIPS code for state', 'Location'),
        ('school_main', 'REGION', 'BIGINT', 'Region (IPEDS)', 'Location'),
        ('school_main', 'LOCALE', 'BIGINT', 'Locale of institution', 'Location'),
        ('school_main', 'LOCALE2', 'VARCHAR', 'Degree of urbanization of institution', 'Location'),
        ('school_main', 'LATITUDE', 'DOUBLE', 'Latitude', 'Location'),
        ('school_main', 'LONGITUDE', 'DOUBLE', 'Longitude', 'Location'),
        ('school_main', 'CURROPER', 'BIGINT', 'Flag for currently operating institution, 0=closed, 1=operating', 'Institutional Characteristics'),
        ('school_main', 'OPENADMP', 'BIGINT', 'Open admissions policy indicator', 'Admissions'),
        ('school_main', 'T4APPROVALDATE', 'DATE', 'Date that institution was first approved to participate in Title IV aid programs', 'Institutional Characteristics'),
        ('school_characteristics', 'SCH_DEG', 'VARCHAR', 'Predominant degree awarded (recoded 0s and 4s)', 'Academics'),
        ('school_characteristics', 'HCM2', 'BIGINT', 'Schools that are on Heightened Cash Monitoring 2 by the Department of Education', 'Institutional Characteristics'),
        ('school_characteristics', 'PREDDEG', 'BIGINT', 'Predominant undergraduate degree awarded', 'Academics'),
        ('school_characteristics', 'HIGHDEG', 'BIGINT', 'Highest degree awarded', 'Academics'),
        ('school_characteristics', 'CCBASIC', 'BIGINT', 'Carnegie Classification -- basic', 'Institutional Characteristics'),
        ('school_characteristics', 'CCUGPROF', 'BIGINT', 'Carnegie Classification -- undergraduate profile', 'Institutional Characteristics'),
        ('school_characteristics', 'CCSIZSET', 'BIGINT', 'Carnegie Classification -- size and setting', 'Institutional Characteristics'),
        ('school_characteristics', 'HBCU', 'BIGINT', 'Flag for Historically Black College and University', 'Institutional Characteristics'),
        ('school_characteristics', 'PBI', 'BIGINT', 'Flag for predominantly black institution', 'Institutional Characteristics'),
        ('school_characteristics', 'ANNHI', 'BIGINT', 'Flag for Alaska Native Native Hawaiian serving institution', 'Institutional Characteristics'),
        ('school_characteristics', 'TRIBAL', 'BIGINT', 'Flag for tribal college and university', 'Institutional Characteristics'),
        ('school_characteristics', 'AANAPII', 'BIGINT', 'Flag for Asian American Native American Pacific Islander-serving institution', 'Institutional Characteristics'),
        ('school_characteristics', 'HSI', 'BIGINT', 'Flag for Hispanic-serving institution', 'Institutional Characteristics'),
        ('school_characteristics', 'NANTI', 'BIGINT', 'Flag for Native American non-tribal institution', 'Institutional Characteristics'),
        ('school_characteristics', 'MENONLY', 'BIGINT', 'Flag for men-only college', 'Institutional Characteristics'),
        ('school_characteristics', 'WOMENONLY', 'BIGINT', 'Flag for women-only college', 'Institutional Characteristics'),
        ('school_characteristics', 'RELAFFIL', 'VARCHAR', 'Religous affiliation of the institution', 'Institutional Characteristics'),
        ('school_characteristics', 'DISTANCEONLY', 'BIGINT', 'Flag for distance-education-only education', 'Institutional Characteristics'),
        ('school_characteristics', 'SCHTYPE', 'BIGINT', 'Control of institution, per PEPS', 'Institutional Characteristics'),
        ('school_characteristics', 'OPEFLAG', 'BIGINT', 'Title IV eligibility type', 'Institutional Characteristics'),
        ('school_characteristics', 'DOLPROVIDER', 'BIGINT', 'DOL approved training provider indicator', 'Institutional Characteristics'),
        ('school_characteristics', 'SCORECARD_SECTOR', 'BIGINT', 'Institutional sector derived from ownership and predominant degree', 'Institutional Characteristics'),
        ('school_admissions', 'ADM_RATE', 'VARCHAR', 'Admission rate', 'Admissions'),
        ('school_admissions', 'ADM_RATE_ALL', 'VARCHAR', 'Admission rate for all campuses rolled up to the 6-digit OPE ID', 'Admissions'),
        ('school_admissions', 'ADM_RATE_SUPP', 'VARCHAR', 'Admission rate, suppressed for n<30', 'Admissions'),
        ('school_admissions', 'SATVR25', 'VARCHAR', '25th percentile of SAT scores (critical reading)', 'Admissions'),
        ('school_admissions', 'SATVR75', 'VARCHAR', '75th percentile of SAT scores (critical reading)', 'Admissions'),
        ('school_admissions', 'SATMT25', 'VARCHAR', '25th percentile of SAT scores (math)', 'Admissions'),
        ('school_admissions', 'SATMT75', 'VARCHAR', '75th percentile of SAT scores (math)', 'Admissions'),
        ('school_admissions', 'SATWR25', 'VARCHAR', '25th percentile of SAT scores (writing)', 'Admissions'),
        ('school_admissions', 'SATWR75', 'VARCHAR', '75th percentile of SAT scores (writing)', 'Admissions'),
        ('school_admissions', 'SATVRMID', 'VARCHAR', 'Midpoint of SAT scores (critical reading)', 'Admissions'),
        ('school_admissions', 'SATMTMID', 'VARCHAR', 'Midpoint of SAT scores (math)', 'Admissions'),
        ('school_admissions', 'SATWRMID', 'VARCHAR', 'Midpoint of SAT scores (writing)', 'Admissions'),
        ('school_admissions', 'ACTCM25', 'VARCHAR', '25th percentile of the ACT cumulative score', 'Admissions'),
        ('school_admissions', 'ACTCM75', 'VARCHAR', '75th percentile of the ACT cumulative score', 'Admissions'),
        ('school_admissions', 'ACTEN25', 'VARCHAR', '25th percentile of the ACT English score', 'Admissions'),
        ('school_admissions', 'ACTEN75', 'VARCHAR', '75th percentile of the ACT English score', 'Admissions'),
        ('school_admissions', 'ACTMT25', 'VARCHAR', '25th percentile of the ACT math score', 'Admissions'),
        ('school_admissions', 'ACTMT75', 'VARCHAR', '75th percentile of the ACT math score', 'Admissions'),
        ('school_admissions', 'ACTWR25', 'VARCHAR', '25th percentile of the ACT writing score', 'Admissions'),
        ('school_admissions', 'ACTWR75', 'VARCHAR', '75th percentile of the ACT writing score', 'Admissions'),
        ('school_admissions', 'ACTCMMID', 'VARCHAR', 'Midpoint of the ACT cumulative score', 'Admissions'),
        ('school_admissions', 'ACTENMID', 'VARCHAR', 'Midpoint of the ACT English score', 'Admissions'),
        ('school_admissions', 'ACTMTMID', 'VARCHAR', 'Midpoint of the ACT math score', 'Admissions'),
        ('school_admissions', 'ACTWRMID', 'VARCHAR', 'Midpoint of the ACT writing score', 'Admissions'),
        ('school_admissions', 'SAT_AVG', 'VARCHAR', 'Average SAT equivalent score of students admitted', 'Admissions'),
        ('school_admissions', 'SAT_AVG_ALL', 'VARCHAR', 'Average SAT equivalent score of students admitted for all campuses', 'Admissions'),
        ('school_admissions', 'ADMCON7', 'VARCHAR', 'Test score requirements for admission', 'Admissions'),
        ('school_academics_cip', 'PCIP01', 'DOUBLE', 'Percentage of degrees in Agriculture', 'Academics'),
        ('school_academics_cip', 'PCIP03', 'DOUBLE', 'Percentage of degrees in Natural Resources and Conservation', 'Academics'),
        ('school_academics_cip', 'PCIP04', 'DOUBLE', 'Percentage of degrees in Architecture', 'Academics'),
        ('school_academics_cip', 'PCIP05', 'DOUBLE', 'Percentage of degrees in Area, Ethnic, Cultural, Gender, and Group Studies', 'Academics'),
        ('school_academics_cip', 'PCIP09', 'DOUBLE', 'Percentage of degrees in Communication and Journalism', 'Academics'),
        ('school_academics_cip', 'PCIP10', 'DOUBLE', 'Percentage of degrees in Communications Technologies', 'Academics'),
        ('school_academics_cip', 'PCIP11', 'DOUBLE', 'Percentage of degrees in Computer and Information Sciences', 'Academics'),
        ('school_academics_cip', 'PCIP12', 'BIGINT', 'Percentage of degrees in Personal and Culinary Services', 'Academics'),
        ('school_academics_cip', 'PCIP13', 'DOUBLE', 'Percentage of degrees in Education', 'Academics'),
        ('school_academics_cip', 'PCIP14', 'DOUBLE', 'Percentage of degrees in Engineering', 'Academics'),
        ('school_academics_cip', 'PCIP15', 'DOUBLE', 'Percentage of degrees in Engineering Technologies', 'Academics'),
        ('school_academics_cip', 'PCIP16', 'DOUBLE', 'Percentage of degrees in Foreign Languages and Linguistics', 'Academics'),
        ('school_academics_cip', 'PCIP19', 'DOUBLE', 'Percentage of degrees in Family and Consumer Sciences/Human Sciences', 'Academics'),
        ('school_academics_cip', 'PCIP22', 'BIGINT', 'Percentage of degrees in Legal Professions and Studies', 'Academics'),
        ('school_academics_cip', 'PCIP23', 'DOUBLE', 'Percentage of degrees in English Language and Literature', 'Academics'),
        ('school_academics_cip', 'PCIP24', 'DOUBLE', 'Percentage of degrees in Liberal Arts and Sciences', 'Academics'),
        ('school_academics_cip', 'PCIP25', 'BIGINT', 'Percentage of degrees in Library Science', 'Academics'),
        ('school_academics_cip', 'PCIP26', 'DOUBLE', 'Percentage of degrees in Biological and Biomedical Sciences', 'Academics'),
        ('school_academics_cip', 'PCIP27', 'DOUBLE', 'Percentage of degrees in Mathematics and Statistics', 'Academics'),
        ('school_academics_cip', 'PCIP29', 'BIGINT', 'Percentage of degrees in Military Technologies', 'Academics'),
        ('school_academics_cip', 'PCIP30', 'DOUBLE', 'Percentage of degrees in Multi/Interdisciplinary Studies', 'Academics'),
        ('school_academics_cip', 'PCIP31', 'DOUBLE', 'Percentage of degrees in Parks, Recreation, Leisure, and Fitness Studies', 'Academics'),
        ('school_academics_cip', 'PCIP38', 'DOUBLE', 'Percentage of degrees in Philosophy and Religious Studies', 'Academics'),
        ('school_academics_cip', 'PCIP39', 'DOUBLE', 'Percentage of degrees in Theology and Religious Vocations', 'Academics'),
        ('school_academics_cip', 'PCIP40', 'DOUBLE', 'Percentage of degrees in Physical Sciences', 'Academics'),
        ('school_academics_cip', 'PCIP41', 'BIGINT', 'Percentage of degrees in Science Technologies', 'Academics'),
        ('school_academics_cip', 'PCIP42', 'DOUBLE', 'Percentage of degrees in Psychology', 'Academics'),
        ('school_academics_cip', 'PCIP43', 'DOUBLE', 'Percentage of degrees in Homeland Security, Law Enforcement, Firefighting', 'Academics'),
        ('school_academics_cip', 'PCIP44', 'DOUBLE', 'Percentage of degrees in Public Administration and Social Service Professions', 'Academics'),
        ('school_academics_cip', 'PCIP45', 'DOUBLE', 'Percentage of degrees in Social Sciences', 'Academics'),
        ('school_academics_cip', 'PCIP46', 'BIGINT', 'Percentage of degrees in Construction Trades', 'Academics'),
        ('school_academics_cip', 'PCIP47', 'BIGINT', 'Percentage of degrees in Mechanic and Repair Technologies', 'Academics'),
        ('school_academics_cip', 'PCIP48', 'BIGINT', 'Percentage of degrees in Precision Production', 'Academics'),
        ('school_academics_cip', 'PCIP49', 'BIGINT', 'Percentage of degrees in Transportation and Materials Moving', 'Academics'),
        ('school_academics_cip', 'PCIP50', 'DOUBLE', 'Percentage of degrees in Visual and Performing Arts', 'Academics'),
        ('school_academics_cip', 'PCIP51', 'DOUBLE', 'Percentage of degrees in Health Professions', 'Academics'),
        ('school_academics_cip', 'PCIP52', 'DOUBLE', 'Percentage of degrees in Business, Management, Marketing', 'Academics'),
        ('school_academics_cip', 'PCIP54', 'DOUBLE', 'Percentage of degrees in History', 'Academics'),
        ('school_academics_cip', 'PRGMOFR', 'VARCHAR', 'Number of programs offered', 'Academics'),
        ('school_academics_cip', 'CIPTITLE1', 'VARCHAR', 'CIP text description of largest program', 'Academics'),
        ('school_academics_cip', 'CIPTITLE2', 'VARCHAR', 'CIP text description of program #2', 'Academics'),
        ('school_academics_cip', 'CIPTITLE3', 'VARCHAR', 'CIP text description of program #3', 'Academics'),
        ('school_academics_cip', 'CIPTITLE4', 'VARCHAR', 'CIP text description of program #4', 'Academics'),
        ('school_academics_cip', 'CIPTITLE5', 'VARCHAR', 'CIP text description of program #5', 'Academics'),
        ('school_academics_cip', 'CIPTITLE6', 'VARCHAR', 'CIP text description of program #6', 'Academics'),
        ('school_student_demographics', 'UGDS', 'BIGINT', 'Enrollment of undergraduate certificate/degree-seeking students', 'Student Demographics'),
        ('school_student_demographics', 'UG', 'VARCHAR', 'Enrollment of all undergraduate students', 'Student Demographics'),
        ('school_student_demographics', 'UGDS_WHITE', 'DOUBLE', 'Share of undergraduate students who are white', 'Student Demographics'),
        ('school_student_demographics', 'UGDS_BLACK', 'DOUBLE', 'Share of undergraduate students who are black', 'Student Demographics'),
        ('school_student_demographics', 'UGDS_HISP', 'DOUBLE', 'Share of undergraduate students who are Hispanic', 'Student Demographics'),
        ('school_student_demographics', 'UGDS_ASIAN', 'DOUBLE', 'Share of undergraduate students who are Asian', 'Student Demographics'),
        ('school_student_demographics', 'UGDS_AIAN', 'DOUBLE', 'Share of undergraduate students who are American Indian/Alaska Native', 'Student Demographics'),
        ('school_student_demographics', 'UGDS_NHPI', 'DOUBLE', 'Share of undergraduate students who are Native Hawaiian/Pacific Islander', 'Student Demographics'),
        ('school_student_demographics', 'UGDS_2MOR', 'DOUBLE', 'Share of undergraduate students who are two or more races', 'Student Demographics'),
        ('school_student_demographics', 'UGDS_NRA', 'DOUBLE', 'Share of undergraduate students who are non-resident aliens', 'Student Demographics'),
        ('school_student_demographics', 'UGDS_UNKN', 'DOUBLE', 'Share of undergraduate students whose race is unknown', 'Student Demographics'),
        ('school_student_demographics', 'UGDS_MEN', 'DOUBLE', 'Share of undergraduate students who are men', 'Student Demographics'),
        ('school_student_demographics', 'UGDS_WOMEN', 'DOUBLE', 'Share of undergraduate students who are women', 'Student Demographics'),
        ('school_student_demographics', 'UGNONDS', 'VARCHAR', 'Number of non-degree-seeking undergraduate students', 'Student Demographics'),
        ('school_student_demographics', 'GRADS', 'BIGINT', 'Number of graduate students', 'Student Demographics'),
        ('school_student_demographics', 'UG12MN', 'BIGINT', 'Unduplicated count of undergraduate students enrolled during a 12 month period', 'Student Demographics'),
        ('school_student_demographics', 'G12MN', 'BIGINT', 'Unduplicated count of graduate students enrolled during a 12 month period', 'Student Demographics'),
        ('school_student_demographics', 'PPTUG_EF', 'DOUBLE', 'Share of undergraduate, degree-/certificate-seeking students who are part-time', 'Student Demographics'),
        ('school_student_demographics', 'UG25ABV', 'DOUBLE', 'Percentage of undergraduates aged 25 and above', 'Student Demographics'),
        ('school_costs', 'COSTT4_A', 'VARCHAR', 'Average cost of attendance (academic year institutions)', 'Costs and Financial Aid'),
        ('school_costs', 'COSTT4_P', 'VARCHAR', 'Average cost of attendance (program-year institutions)', 'Costs and Financial Aid'),
        ('school_costs', 'TUITIONFEE_IN', 'VARCHAR', 'In-state tuition and fees', 'Costs and Financial Aid'),
        ('school_costs', 'TUITIONFEE_OUT', 'VARCHAR', 'Out-of-state tuition and fees', 'Costs and Financial Aid'),
        ('school_costs', 'TUITIONFEE_PROG', 'VARCHAR', 'Tuition and fees for program-year institutions', 'Costs and Financial Aid'),
        ('school_costs', 'TUITFTE', 'BIGINT', 'Net tuition revenue per full-time equivalent student', 'Costs and Financial Aid'),
        ('school_costs', 'INEXPFTE', 'BIGINT', 'Instructional expenditures per full-time equivalent student', 'Costs and Financial Aid'),
        ('school_costs', 'BOOKSUPPLY', 'VARCHAR', 'Cost of attendance: estimated books and supplies', 'Costs and Financial Aid'),
        ('school_costs', 'ROOMBOARD_ON', 'VARCHAR', 'Cost of attendance: on-campus room and board', 'Costs and Financial Aid'),
        ('school_costs', 'OTHEREXPENSE_ON', 'VARCHAR', 'Cost of attendance: on-campus other expenses', 'Costs and Financial Aid'),
        ('school_costs', 'ROOMBOARD_OFF', 'VARCHAR', 'Cost of attendance: off-campus room and board', 'Costs and Financial Aid'),
        ('school_costs', 'OTHEREXPENSE_OFF', 'VARCHAR', 'Cost of attendance: off-campus other expenses', 'Costs and Financial Aid'),
        ('school_costs', 'OTHEREXPENSE_FAM', 'VARCHAR', 'Cost of attendance: with-family other expenses', 'Costs and Financial Aid'),
        ('school_financial_aid', 'NPT4_PUB', 'VARCHAR', 'Average net price for Title IV institutions (public institutions)', 'Costs and Financial Aid'),
        ('school_financial_aid', 'NPT4_PRIV', 'VARCHAR', 'Average net price for Title IV institutions (private institutions)', 'Costs and Financial Aid'),
        ('school_financial_aid', 'NPT4_PROG', 'VARCHAR', 'Average net price for program-year institutions', 'Costs and Financial Aid'),
        ('school_financial_aid', 'NPT4_OTHER', 'VARCHAR', 'Average net price for other academic year calendars', 'Costs and Financial Aid'),
        ('school_financial_aid', 'NPT41_PUB', 'VARCHAR', 'Average net price for $0-$30,000 family income (public)', 'Costs and Financial Aid'),
        ('school_financial_aid', 'NPT42_PUB', 'VARCHAR', 'Average net price for $30,001-$48,000 family income (public)', 'Costs and Financial Aid'),
        ('school_financial_aid', 'NPT43_PUB', 'VARCHAR', 'Average net price for $48,001-$75,000 family income (public)', 'Costs and Financial Aid'),
        ('school_financial_aid', 'NPT44_PUB', 'VARCHAR', 'Average net price for $75,001-$110,000 family income (public)', 'Costs and Financial Aid'),
        ('school_financial_aid', 'NPT45_PUB', 'VARCHAR', 'Average net price for $110,000+ family income (public)', 'Costs and Financial Aid'),
        ('school_financial_aid', 'NPT41_PRIV', 'VARCHAR', 'Average net price for $0-$30,000 family income (private)', 'Costs and Financial Aid'),
        ('school_financial_aid', 'NPT42_PRIV', 'VARCHAR', 'Average net price for $30,001-$48,000 family income (private)', 'Costs and Financial Aid'),
        ('school_financial_aid', 'NPT43_PRIV', 'VARCHAR', 'Average net price for $48,001-$75,000 family income (private)', 'Costs and Financial Aid'),
        ('school_financial_aid', 'NPT44_PRIV', 'VARCHAR', 'Average net price for $75,001-$110,000 family income (private)', 'Costs and Financial Aid'),
        ('school_financial_aid', 'NPT45_PRIV', 'VARCHAR', 'Average net price for $110,000+ family income (private)', 'Costs and Financial Aid'),
        ('school_financial_aid', 'PCTPELL', 'DOUBLE', 'Percentage of undergraduates who receive a Pell Grant', 'Costs and Financial Aid'),
        ('school_financial_aid', 'PCTFLOAN', 'DOUBLE', 'Percent of all undergraduates receiving a federal student loan', 'Costs and Financial Aid'),
        ('school_financial_aid', 'FTFTPCTPELL', 'VARCHAR', 'Percentage of full-time, first-time undergraduates awarded a Pell Grant', 'Costs and Financial Aid'),
        ('school_financial_aid', 'FTFTPCTFLOAN', 'VARCHAR', 'Percentage of full-time, first-time undergraduates awarded a federal loan', 'Costs and Financial Aid'),
        ('school_completion_rates', 'C150_4', 'DOUBLE', 'Completion rate at four-year institutions (150%)', 'Completion and Retention'),
        ('school_completion_rates', 'C150_L4', 'VARCHAR', 'Completion rate at less-than-four-year institutions (150%)', 'Completion and Retention'),
        ('school_completion_rates', 'C150_4_POOLED', 'DOUBLE', 'Pooled completion rate at four-year institutions (150%)', 'Completion and Retention'),
        ('school_completion_rates', 'C150_L4_POOLED', 'VARCHAR', 'Pooled completion rate at less-than-four-year institutions (150%)', 'Completion and Retention'),
        ('school_completion_rates', 'C200_4', 'DOUBLE', 'Completion rate at four-year institutions (200%)', 'Completion and Retention'),
        ('school_completion_rates', 'C200_L4', 'VARCHAR', 'Completion rate at less-than-four-year institutions (200%)', 'Completion and Retention'),
        ('school_completion_rates', 'C100_4', 'DOUBLE', 'Completion rate at four-year institutions (100%)', 'Completion and Retention'),
        ('school_completion_rates', 'C100_L4', 'VARCHAR', 'Completion rate at less-than-four-year institutions (100%)', 'Completion and Retention'),
        ('school_completion_rates', 'OMAWDP6_FTFT', 'DOUBLE', 'Award rate for full-time, first-time students (6 years)', 'Completion and Retention'),
        ('school_completion_rates', 'OMAWDP8_FTFT', 'DOUBLE', 'Award rate for full-time, first-time students (8 years)', 'Completion and Retention'),
        ('school_completion_rates', 'OMAWDP6_PTFT', 'DOUBLE', 'Award rate for part-time, first-time students (6 years)', 'Completion and Retention'),
        ('school_completion_rates', 'OMAWDP8_PTFT', 'DOUBLE', 'Award rate for part-time, first-time students (8 years)', 'Completion and Retention'),
        ('school_completion_rates', 'OMAWDP6_FTNFT', 'DOUBLE', 'Award rate for full-time, not first-time students (6 years)', 'Completion and Retention'),
        ('school_completion_rates', 'OMAWDP8_FTNFT', 'DOUBLE', 'Award rate for full-time, not first-time students (8 years)', 'Completion and Retention'),
        ('school_completion_rates', 'OMAWDP6_PTNFT', 'DOUBLE', 'Award rate for part-time, not first-time students (6 years)', 'Completion and Retention'),
        ('school_completion_rates', 'OMAWDP8_PTNFT', 'DOUBLE', 'Award rate for part-time, not first-time students (8 years)', 'Completion and Retention'),
        ('school_retention_rates', 'RET_FT4', 'VARCHAR', 'First-time, full-time retention rate at four-year institutions', 'Completion and Retention'),
        ('school_retention_rates', 'RET_FTL4', 'VARCHAR', 'First-time, full-time retention rate at less-than-four-year institutions', 'Completion and Retention'),
        ('school_retention_rates', 'RET_PT4', 'VARCHAR', 'First-time, part-time retention rate at four-year institutions', 'Completion and Retention'),
        ('school_retention_rates', 'RET_PTL4', 'VARCHAR', 'First-time, part-time retention rate at less-than-four-year institutions', 'Completion and Retention'),
        ('school_student_debt', 'DEBT_MDN', 'VARCHAR', 'The median original loan principal upon entering repayment', 'Debt and Repayment'),
        ('school_student_debt', 'GRAD_DEBT_MDN', 'VARCHAR', 'The median debt for students who have completed', 'Debt and Repayment'),
        ('school_student_debt', 'WDRAW_DEBT_MDN', 'VARCHAR', 'The median debt for students who have not completed', 'Debt and Repayment'),
        ('school_student_debt', 'LO_INC_DEBT_MDN', 'VARCHAR', 'Median debt for students with family income $0-$30,000', 'Debt and Repayment'),
        ('school_student_debt', 'MD_INC_DEBT_MDN', 'VARCHAR', 'Median debt for students with family income $30,001-$75,000', 'Debt and Repayment'),
        ('school_student_debt', 'HI_INC_DEBT_MDN', 'VARCHAR', 'Median debt for students with family income $75,001+', 'Debt and Repayment'),
        ('school_student_debt', 'DEP_DEBT_MDN', 'VARCHAR', 'The median debt for dependent students', 'Debt and Repayment'),
        ('school_student_debt', 'IND_DEBT_MDN', 'VARCHAR', 'The median debt for independent students', 'Debt and Repayment'),
        ('school_student_debt', 'PELL_DEBT_MDN', 'VARCHAR', 'The median debt for Pell students', 'Debt and Repayment'),
        ('school_student_debt', 'NOPELL_DEBT_MDN', 'VARCHAR', 'The median debt for no-Pell students', 'Debt and Repayment'),
        ('school_student_debt', 'FEMALE_DEBT_MDN', 'VARCHAR', 'The median debt for female students', 'Debt and Repayment'),
        ('school_student_debt', 'MALE_DEBT_MDN', 'VARCHAR', 'The median debt for male students', 'Debt and Repayment'),
        ('school_student_debt', 'FIRSTGEN_DEBT_MDN', 'VARCHAR', 'The median debt for first-generation students', 'Debt and Repayment'),
        ('school_student_debt', 'NOTFIRSTGEN_DEBT_MDN', 'VARCHAR', 'The median debt for not-first-generation students', 'Debt and Repayment'),
        ('school_student_debt', 'GRAD_DEBT_MDN10YR', 'VARCHAR', 'Median loan debt of completers in monthly payments (10-year plan)', 'Debt and Repayment'),
        ('school_repayment_rates', 'RPY_1YR_RT', 'VARCHAR', 'One-year repayment rate', 'Debt and Repayment'),
        ('school_repayment_rates', 'COMPL_RPY_1YR_RT', 'VARCHAR', 'One-year repayment rate for completers', 'Debt and Repayment'),
        ('school_repayment_rates', 'NONCOM_RPY_1YR_RT', 'VARCHAR', 'One-year repayment rate for non-completers', 'Debt and Repayment'),
        ('school_repayment_rates', 'RPY_3YR_RT', 'VARCHAR', 'Three-year repayment rate', 'Debt and Repayment'),
        ('school_repayment_rates', 'COMPL_RPY_3YR_RT', 'VARCHAR', 'Three-year repayment rate for completers', 'Debt and Repayment'),
        ('school_repayment_rates', 'NONCOM_RPY_3YR_RT', 'VARCHAR', 'Three-year repayment rate for non-completers', 'Debt and Repayment'),
        ('school_repayment_rates', 'RPY_5YR_RT', 'VARCHAR', 'Five-year repayment rate', 'Debt and Repayment'),
        ('school_repayment_rates', 'COMPL_RPY_5YR_RT', 'VARCHAR', 'Five-year repayment rate for completers', 'Debt and Repayment'),
        ('school_repayment_rates', 'NONCOM_RPY_5YR_RT', 'VARCHAR', 'Five-year repayment rate for non-completers', 'Debt and Repayment'),
        ('school_repayment_rates', 'RPY_7YR_RT', 'VARCHAR', 'Seven-year repayment rate', 'Debt and Repayment'),
        ('school_repayment_rates', 'COMPL_RPY_7YR_RT', 'VARCHAR', 'Seven-year repayment rate for completers', 'Debt and Repayment'),
        ('school_repayment_rates', 'NONCOM_RPY_7YR_RT', 'VARCHAR', 'Seven-year repayment rate for non-completers', 'Debt and Repayment'),
        ('school_repayment_rates', 'CDR2', 'VARCHAR', 'Two-year cohort default rate', 'Debt and Repayment'),
        ('school_repayment_rates', 'CDR3', 'DOUBLE', 'Three-year cohort default rate', 'Debt and Repayment'),
        ('school_earnings_p6', 'COUNT_NWNE_P6', 'VARCHAR', 'Number of students not working and not enrolled 6 years after entry', 'Earnings'),
        ('school_earnings_p6', 'COUNT_WNE_P6', 'VARCHAR', 'Number of students working and not enrolled 6 years after entry', 'Earnings'),
        ('school_earnings_p6', 'MN_EARN_WNE_P6', 'VARCHAR', 'Mean earnings of students working and not enrolled 6 years after entry', 'Earnings'),
        ('school_earnings_p6', 'MD_EARN_WNE_P6', 'VARCHAR', 'Median earnings of students working and not enrolled 6 years after entry', 'Earnings'),
        ('school_earnings_p6', 'GT_25K_P6', 'VARCHAR', 'Share of students earning over $25,000/year 6 years after entry', 'Earnings'),
        ('school_earnings_p6', 'GT_28K_P6', 'VARCHAR', 'Share of students earning over $28,000/year 6 years after entry', 'Earnings'),
        ('school_earnings_p8', 'COUNT_NWNE_P8', 'VARCHAR', 'Number of students not working and not enrolled 8 years after entry', 'Earnings'),
        ('school_earnings_p8', 'COUNT_WNE_P8', 'VARCHAR', 'Number of students working and not enrolled 8 years after entry', 'Earnings'),
        ('school_earnings_p8', 'MN_EARN_WNE_P8', 'VARCHAR', 'Mean earnings of students working and not enrolled 8 years after entry', 'Earnings'),
        ('school_earnings_p8', 'MD_EARN_WNE_P8', 'VARCHAR', 'Median earnings of students working and not enrolled 8 years after entry', 'Earnings'),
        ('school_earnings_p8', 'GT_25K_P8', 'VARCHAR', 'Share of students earning over $25,000/year 8 years after entry', 'Earnings'),
        ('school_earnings_p8', 'GT_28K_P8', 'VARCHAR', 'Share of students earning over $28,000/year 8 years after entry', 'Earnings'),
        ('school_earnings_p10', 'COUNT_NWNE_P10', 'VARCHAR', 'Number of students not working and not enrolled 10 years after entry', 'Earnings'),
        ('school_earnings_p10', 'COUNT_WNE_P10', 'VARCHAR', 'Number of students working and not enrolled 10 years after entry', 'Earnings'),
        ('school_earnings_p10', 'MN_EARN_WNE_P10', 'VARCHAR', 'Mean earnings of students working and not enrolled 10 years after entry', 'Earnings'),
        ('school_earnings_p10', 'MD_EARN_WNE_P10', 'VARCHAR', 'Median earnings of students working and not enrolled 10 years after entry', 'Earnings'),
        ('school_earnings_p10', 'GT_25K_P10', 'VARCHAR', 'Share of students earning over $25,000/year 10 years after entry', 'Earnings'),
        ('school_earnings_p10', 'GT_28K_P10', 'VARCHAR', 'Share of students earning over $28,000/year 10 years after entry', 'Earnings'),
        ('school_faculty', 'AVGFACSAL', 'BIGINT', 'Average faculty salary', 'Faculty'),
        ('school_faculty', 'PFTFAC', 'DOUBLE', 'Proportion of faculty that is full-time', 'Faculty'),
        ('school_faculty', 'STUFACR', 'BIGINT', 'Undergraduate student to instructional faculty ratio', 'Faculty'),
        ('school_faculty', 'IRPS_2MOR', 'DOUBLE', 'Share of full time faculty that are Two or More Races', 'Faculty'),
        ('school_faculty', 'IRPS_AIAN', 'DOUBLE', 'Share of full time faculty that are American Indian or Alaskan Native', 'Faculty'),
        ('school_faculty', 'IRPS_ASIAN', 'DOUBLE', 'Share of full time faculty that are Asian', 'Faculty'),
        ('school_faculty', 'IRPS_BLACK', 'DOUBLE', 'Share of full time faculty that are Black or African American', 'Faculty'),
        ('school_faculty', 'IRPS_HISP', 'DOUBLE', 'Share of full time faculty that are Hispanic', 'Faculty'),
        ('school_faculty', 'IRPS_NHPI', 'DOUBLE', 'Share of full time faculty that are Native Hawaiian or Other Pacific Islander', 'Faculty'),
        ('school_faculty', 'IRPS_NRA', 'DOUBLE', 'Share of full time faculty that are U.S. Nonresidents', 'Faculty'),
        ('school_faculty', 'IRPS_UNKN', 'DOUBLE', 'Share of full time faculty that are of unknown race/ethnicity', 'Faculty'),
        ('school_faculty', 'IRPS_WHITE', 'DOUBLE', 'Share of full time faculty that are White', 'Faculty'),
        ('school_faculty', 'IRPS_WOMEN', 'DOUBLE', 'Share of full time faculty that are women', 'Faculty'),
        ('school_faculty', 'IRPS_MEN', 'DOUBLE', 'Share of full time faculty that are men', 'Faculty')
    ]
    cursor.executemany("INSERT INTO schema_information VALUES (?, ?, ?, ?, ?)", schema_info_inserts)


    conn.commit()
    conn.close()
    print("Database and tables created successfully.")

def load_csv_data(csv_file_path, db_name="schools.db"):
    """
    Loads data from a CSV file into the SQLite database.
    """
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()

    # Define the columns for each table
    table_columns = {
        "school_main": ['UNITID', 'OPEID', 'OPEID6', 'INSTNM', 'CITY', 'STABBR', 'ZIP', 'ADDR', 'ACCREDAGENCY', 'ACCREDCODE', 'INSTURL', 'NPCURL', 'MAIN', 'NUMBRANCH', 'CONTROL', 'ST_FIPS', 'REGION', 'LOCALE', 'LOCALE2', 'LATITUDE', 'LONGITUDE', 'CURROPER', 'OPENADMP', 'T4APPROVALDATE'],
        "school_characteristics": ['UNITID', 'SCH_DEG', 'HCM2', 'PREDDEG', 'HIGHDEG', 'CCBASIC', 'CCUGPROF', 'CCSIZSET', 'HBCU', 'PBI', 'ANNHI', 'TRIBAL', 'AANAPII', 'HSI', 'NANTI', 'MENONLY', 'WOMENONLY', 'RELAFFIL', 'DISTANCEONLY', 'SCHTYPE', 'OPEFLAG', 'DOLPROVIDER', 'SCORECARD_SECTOR'],
        "school_admissions": ['UNITID', 'ADM_RATE', 'ADM_RATE_ALL', 'ADM_RATE_SUPP', 'SATVR25', 'SATVR75', 'SATMT25', 'SATMT75', 'SATWR25', 'SATWR75', 'SATVRMID', 'SATMTMID', 'SATWRMID', 'ACTCM25', 'ACTCM75', 'ACTEN25', 'ACTEN75', 'ACTMT25', 'ACTMT75', 'ACTWR25', 'ACTWR75', 'ACTCMMID', 'ACTENMID', 'ACTMTMID', 'ACTWRMID', 'SAT_AVG', 'SAT_AVG_ALL', 'ADMCON7'],
        "school_academics_cip": ['UNITID', 'PCIP01', 'PCIP03', 'PCIP04', 'PCIP05', 'PCIP09', 'PCIP10', 'PCIP11', 'PCIP12', 'PCIP13', 'PCIP14', 'PCIP15', 'PCIP16', 'PCIP19', 'PCIP22', 'PCIP23', 'PCIP24', 'PCIP25', 'PCIP26', 'PCIP27', 'PCIP29', 'PCIP30', 'PCIP31', 'PCIP38', 'PCIP39', 'PCIP40', 'PCIP41', 'PCIP42', 'PCIP43', 'PCIP44', 'PCIP45', 'PCIP46', 'PCIP47', 'PCIP48', 'PCIP49', 'PCIP50', 'PCIP51', 'PCIP52', 'PCIP54', 'PRGMOFR', 'CIPTITLE1', 'CIPTITLE2', 'CIPTITLE3', 'CIPTITLE4', 'CIPTITLE5', 'CIPTITLE6'],
        "school_student_demographics": ['UNITID', 'UGDS', 'UG', 'UGDS_WHITE', 'UGDS_BLACK', 'UGDS_HISP', 'UGDS_ASIAN', 'UGDS_AIAN', 'UGDS_NHPI', 'UGDS_2MOR', 'UGDS_NRA', 'UGDS_UNKN', 'UGDS_MEN', 'UGDS_WOMEN', 'UGNONDS', 'GRADS', 'UG12MN', 'G12MN', 'PPTUG_EF', 'UG25ABV'],
        "school_costs": ['UNITID', 'COSTT4_A', 'COSTT4_P', 'TUITIONFEE_IN', 'TUITIONFEE_OUT', 'TUITIONFEE_PROG', 'TUITFTE', 'INEXPFTE', 'BOOKSUPPLY', 'ROOMBOARD_ON', 'OTHEREXPENSE_ON', 'ROOMBOARD_OFF', 'OTHEREXPENSE_OFF', 'OTHEREXPENSE_FAM'],
        "school_financial_aid": ['UNITID', 'NPT4_PUB', 'NPT4_PRIV', 'NPT4_PROG', 'NPT4_OTHER', 'NPT41_PUB', 'NPT42_PUB', 'NPT43_PUB', 'NPT44_PUB', 'NPT45_PUB', 'NPT41_PRIV', 'NPT42_PRIV', 'NPT43_PRIV', 'NPT44_PRIV', 'NPT45_PRIV', 'PCTPELL', 'PCTFLOAN', 'FTFTPCTPELL', 'FTFTPCTFLOAN'],
        "school_completion_rates": ['UNITID', 'C150_4', 'C150_L4', 'C150_4_POOLED', 'C150_L4_POOLED', 'C200_4', 'C200_L4', 'C100_4', 'C100_L4', 'OMAWDP6_FTFT', 'OMAWDP8_FTFT', 'OMAWDP6_PTFT', 'OMAWDP8_PTFT', 'OMAWDP6_FTNFT', 'OMAWDP8_FTNFT', 'OMAWDP6_PTNFT', 'OMAWDP8_PTNFT'],
        "school_retention_rates": ['UNITID', 'RET_FT4', 'RET_FTL4', 'RET_PT4', 'RET_PTL4'],
        "school_student_debt": ['UNITID', 'DEBT_MDN', 'GRAD_DEBT_MDN', 'WDRAW_DEBT_MDN', 'LO_INC_DEBT_MDN', 'MD_INC_DEBT_MDN', 'HI_INC_DEBT_MDN', 'DEP_DEBT_MDN', 'IND_DEBT_MDN', 'PELL_DEBT_MDN', 'NOPELL_DEBT_MDN', 'FEMALE_DEBT_MDN', 'MALE_DEBT_MDN', 'FIRSTGEN_DEBT_MDN', 'NOTFIRSTGEN_DEBT_MDN', 'GRAD_DEBT_MDN10YR'],
        "school_repayment_rates": ['UNITID', 'RPY_1YR_RT', 'COMPL_RPY_1YR_RT', 'NONCOM_RPY_1YR_RT', 'RPY_3YR_RT', 'COMPL_RPY_3YR_RT', 'NONCOM_RPY_3YR_RT', 'RPY_5YR_RT', 'COMPL_RPY_5YR_RT', 'NONCOM_RPY_5YR_RT', 'RPY_7YR_RT', 'COMPL_RPY_7YR_RT', 'NONCOM_RPY_7YR_RT', 'CDR2', 'CDR3'],
        "school_earnings_p6": ['UNITID', 'COUNT_NWNE_P6', 'COUNT_WNE_P6', 'MN_EARN_WNE_P6', 'MD_EARN_WNE_P6', 'GT_25K_P6', 'GT_28K_P6'],
        "school_earnings_p8": ['UNITID', 'COUNT_NWNE_P8', 'COUNT_WNE_P8', 'MN_EARN_WNE_P8', 'MD_EARN_WNE_P8', 'GT_25K_P8', 'GT_28K_P8'],
        "school_earnings_p10": ['UNITID', 'COUNT_NWNE_P10', 'COUNT_WNE_P10', 'MN_EARN_WNE_P10', 'MD_EARN_WNE_P10', 'GT_25K_P10', 'GT_28K_P10'],
        "school_faculty": ['UNITID', 'AVGFACSAL', 'PFTFAC', 'STUFACR', 'IRPS_2MOR', 'IRPS_AIAN', 'IRPS_ASIAN', 'IRPS_BLACK', 'IRPS_HISP', 'IRPS_NHPI', 'IRPS_NRA', 'IRPS_UNKN', 'IRPS_WHITE', 'IRPS_WOMEN', 'IRPS_MEN']
    }

    try:
        with open(csv_file_path, 'r', encoding='utf-8-sig') as csv_file:
            reader = csv.reader(csv_file)
            header = next(reader)
            header_map = {col_name: idx for idx, col_name in enumerate(header)}

            for row in reader:
                for table_name, columns in table_columns.items():
                    # Prepare the data tuple for insertion
                    values_to_insert = []
                    for col in columns:
                        # Find the index of the column in the original CSV
                        idx = header_map.get(col)
                        if idx is not None:
                            value = row[idx]
                            # Replace empty strings and 'NULL' values with None for database NULL
                            if value == '' or value.upper() == 'NULL':
                                values_to_insert.append(None)
                            else:
                                values_to_insert.append(value)
                        else:
                            # If a column is missing in the CSV, append None
                            values_to_insert.append(None)
                    
                    # Create the INSERT statement
                    placeholders = ', '.join(['?'] * len(columns))
                    sql = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})"
                    # print(f"SQL: {sql}")
                    # print(f"Values to insert: {values_to_insert}")
                    
                    try:
                        cursor.execute(sql, tuple(values_to_insert))
                        # print(f"Inserted into {table_name}: {values_to_insert}")
                        conn.commit()
                    except sqlite3.IntegrityError as e:
                        print(f"Skipping duplicate UNITID: {values_to_insert[0]} in table {table_name}")
                    except Exception as e:
                        print(f"An error occurred inserting into {table_name}: {e}")
                        print(f"Data that caused error: {values_to_insert}")


    except FileNotFoundError:
        print(f"Error: The file {csv_file_path} was not found.")
        return
    except Exception as e:
        print(f"An error occurred: {e}")
        return

    conn.commit()
    conn.close()
    print("Data loaded successfully from CSV into the database.")

def fix_date_columns_in_db():
    """
    One-time script to convert date columns from DD-MM-YYYY to YYYY-MM-DD
    WARNING: This modifies your database. Make a backup first!
    """
    from datetime import datetime
    
    engine = create_engine("sqlite:///schools.db")
    
    with engine.connect() as conn:
        # Example for a specific table - adjust table and column names
        # Get all records with dates
        result = conn.execute(text("SELECT rowid, T4APPROVALDATE FROM school_main"))
        
        for row in result:
            old_date = row[1]
            print(f"Processing rowid {row[0]} with date {old_date}")
            if old_date and '-' in old_date:
                try:
                    # Parse DD-MM-YYYY
                    parts = old_date.split('-')
                    if len(parts) == 3 and len(parts[2]) == 4:
                        # Convert to YYYY-MM-DD
                        new_date = f"{parts[2]}-{parts[1]}-{parts[0]}"
                        conn.execute(
                            text("UPDATE school_main SET T4APPROVALDATE = :new WHERE rowid = :id"),
                            {"new": new_date, "id": row[0]}
                        )
                        print(f"Updated rowid {row[0]}: {old_date} -> {new_date}")
                except Exception as e:
                    print(f"Could not convert date {old_date}: {e}")
        
        conn.commit()


if __name__ == "__main__":
    # --- Configuration ---
    DATABASE_NAME = "schools.db"
    # IMPORTANT: Replace 'your_schools_data.csv' with the actual name of your CSV file.
    CSV_FILE_PATH = "schools_main.csv"

    # Clean up any existing database file
    delete_db(DATABASE_NAME)

    # 1. Create the database and tables
    create_database_and_tables(DATABASE_NAME)

    # 2. Load data from the CSV file into the tables
    load_csv_data(CSV_FILE_PATH, DATABASE_NAME)

    # # 3. Fix date columns in the database
    # fix_date_columns_in_db() 

    conn = sqlite3.connect(DATABASE_NAME)
    cursor = conn.cursor()

    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cursor.fetchall()

    for table in tables:
        cursor.execute(f"select count(*) from {table[0]}")
        count = cursor.fetchone()[0]
        print(f"Table {table} has {count} records.")
    

    
    conn.close()


# Uncomment to run the fix (MAKE A BACKUP FIRST!)
# fix_date_columns_in_db()