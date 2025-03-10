#!/usr/bin/env python
# coding: utf-8

# In[3]:


import pandas
import os

# This query represents dataset "biomarkers" for domain "person" and was generated for All of Us Registered Tier Dataset v8
dataset_26024896_person_sql = """
    SELECT
        person.person_id,
        p_gender_concept.concept_name as gender,
        p_race_concept.concept_name as race 
    FROM
        `""" + os.environ["WORKSPACE_CDR"] + """.person` person 
    LEFT JOIN
        `""" + os.environ["WORKSPACE_CDR"] + """.concept` p_gender_concept 
            ON person.gender_concept_id = p_gender_concept.concept_id 
    LEFT JOIN
        `""" + os.environ["WORKSPACE_CDR"] + """.concept` p_race_concept 
            ON person.race_concept_id = p_race_concept.concept_id  
    WHERE
        person.PERSON_ID IN (SELECT
            distinct person_id  
        FROM
            `""" + os.environ["WORKSPACE_CDR"] + """.cb_search_person` cb_search_person  
        WHERE
            cb_search_person.person_id IN (SELECT
                criteria.person_id 
            FROM
                (SELECT
                    DISTINCT person_id, entry_date, concept_id 
                FROM
                    `""" + os.environ["WORKSPACE_CDR"] + """.cb_search_all_events` 
                WHERE
                    (concept_id IN(SELECT
                        DISTINCT c.concept_id 
                    FROM
                        `""" + os.environ["WORKSPACE_CDR"] + """.cb_criteria` c 
                    JOIN
                        (SELECT
                            CAST(cr.id as string) AS id       
                        FROM
                            `""" + os.environ["WORKSPACE_CDR"] + """.cb_criteria` cr       
                        WHERE
                            concept_id IN (3004410, 40772572, 40785865)       
                            AND full_text LIKE '%_rank1]%'      ) a 
                            ON (c.path LIKE CONCAT('%.', a.id, '.%') 
                            OR c.path LIKE CONCAT('%.', a.id) 
                            OR c.path LIKE CONCAT(a.id, '.%') 
                            OR c.path = a.id) 
                    WHERE
                        is_standard = 1 
                        AND is_selectable = 1) 
                    AND is_standard = 1 )) criteria ) 
            AND cb_search_person.person_id IN (SELECT
                person_id 
            FROM
                `""" + os.environ["WORKSPACE_CDR"] + """.cb_search_person` p 
            WHERE
                DATE_DIFF(CURRENT_DATE, dob, YEAR) - IF(EXTRACT(MONTH FROM dob)*100 + EXTRACT(DAY FROM dob) > EXTRACT(MONTH FROM CURRENT_DATE)*100 + EXTRACT(DAY FROM CURRENT_DATE), 1, 0) BETWEEN 18 AND 70 
                AND NOT EXISTS (      SELECT
                    'x'      
                FROM
                    `""" + os.environ["WORKSPACE_CDR"] + """.death` d      
                WHERE
                    d.person_id = p.person_id ) ) 
            AND cb_search_person.person_id IN (SELECT
                person_id 
            FROM
                `""" + os.environ["WORKSPACE_CDR"] + """.person` p 
            WHERE
                gender_concept_id IN (45878463, 45880669) ) )"""

dataset_26024896_person_df = pandas.read_gbq(
    dataset_26024896_person_sql,
    dialect="standard",
    use_bqstorage_api=("BIGQUERY_STORAGE_API_ENABLED" in os.environ),
    progress_bar_type="tqdm_notebook")

dataset_26024896_person_df.head(5)


# In[4]:


import pandas
import os

# This query represents dataset "biomarkers" for domain "measurement" and was generated for All of Us Registered Tier Dataset v8
dataset_34795417_measurement_sql = """
    SELECT
        measurement.person_id,
        m_standard_concept.concept_name as standard_concept_name,
        m_standard_concept.concept_code as standard_concept_code,
        measurement.value_as_number,
        m_value.concept_name as value_as_concept_name,
        m_source_concept.concept_name as source_concept_name 
    FROM
        ( SELECT
            * 
        FROM
            `""" + os.environ["WORKSPACE_CDR"] + """.measurement` measurement 
        WHERE
            (
                measurement_concept_id IN (SELECT
                    DISTINCT c.concept_id 
                FROM
                    `""" + os.environ["WORKSPACE_CDR"] + """.cb_criteria` c 
                JOIN
                    (SELECT
                        CAST(cr.id as string) AS id       
                    FROM
                        `""" + os.environ["WORKSPACE_CDR"] + """.cb_criteria` cr       
                    WHERE
                        concept_id IN (3004410, 3022192, 40785865)       
                        AND full_text LIKE '%_rank1]%'      ) a 
                        ON (c.path LIKE CONCAT('%.', a.id, '.%') 
                        OR c.path LIKE CONCAT('%.', a.id) 
                        OR c.path LIKE CONCAT(a.id, '.%') 
                        OR c.path = a.id) 
                WHERE
                    is_standard = 1 
                    AND is_selectable = 1)
            )  
            AND (
                measurement.PERSON_ID IN (SELECT
                    distinct person_id  
                FROM
                    `""" + os.environ["WORKSPACE_CDR"] + """.cb_search_person` cb_search_person  
                WHERE
                    cb_search_person.person_id IN (SELECT
                        criteria.person_id 
                    FROM
                        (SELECT
                            DISTINCT person_id, entry_date, concept_id 
                        FROM
                            `""" + os.environ["WORKSPACE_CDR"] + """.cb_search_all_events` 
                        WHERE
                            (concept_id IN(SELECT
                                DISTINCT c.concept_id 
                            FROM
                                `""" + os.environ["WORKSPACE_CDR"] + """.cb_criteria` c 
                            JOIN
                                (SELECT
                                    CAST(cr.id as string) AS id       
                                FROM
                                    `""" + os.environ["WORKSPACE_CDR"] + """.cb_criteria` cr       
                                WHERE
                                    concept_id IN (3004410, 40772572, 40785865)       
                                    AND full_text LIKE '%_rank1]%'      ) a 
                                    ON (c.path LIKE CONCAT('%.', a.id, '.%') 
                                    OR c.path LIKE CONCAT('%.', a.id) 
                                    OR c.path LIKE CONCAT(a.id, '.%') 
                                    OR c.path = a.id) 
                            WHERE
                                is_standard = 1 
                                AND is_selectable = 1) 
                            AND is_standard = 1 )) criteria ) 
                    AND cb_search_person.person_id IN (SELECT
                        person_id 
                    FROM
                        `""" + os.environ["WORKSPACE_CDR"] + """.cb_search_person` p 
                    WHERE
                        DATE_DIFF(CURRENT_DATE, dob, YEAR) - IF(EXTRACT(MONTH FROM dob)*100 + EXTRACT(DAY FROM dob) > EXTRACT(MONTH FROM CURRENT_DATE)*100 + EXTRACT(DAY FROM CURRENT_DATE), 1, 0) BETWEEN 18 AND 70 
                        AND NOT EXISTS (      SELECT
                            'x'      
                        FROM
                            `""" + os.environ["WORKSPACE_CDR"] + """.death` d      
                        WHERE
                            d.person_id = p.person_id ) ) 
                    AND cb_search_person.person_id IN (SELECT
                        person_id 
                    FROM
                        `""" + os.environ["WORKSPACE_CDR"] + """.person` p 
                    WHERE
                        gender_concept_id IN (45878463, 45880669) ) )
            )) measurement 
    LEFT JOIN
        `""" + os.environ["WORKSPACE_CDR"] + """.concept` m_standard_concept 
            ON measurement.measurement_concept_id = m_standard_concept.concept_id 
    LEFT JOIN
        `""" + os.environ["WORKSPACE_CDR"] + """.concept` m_value 
            ON measurement.value_as_concept_id = m_value.concept_id 
    LEFT JOIN
        `""" + os.environ["WORKSPACE_CDR"] + """.concept` m_source_concept 
            ON measurement.measurement_source_concept_id = m_source_concept.concept_id"""

dataset_34795417_measurement_df = pandas.read_gbq(
    dataset_34795417_measurement_sql,
    dialect="standard",
    use_bqstorage_api=("BIGQUERY_STORAGE_API_ENABLED" in os.environ),
    progress_bar_type="tqdm_notebook")

dataset_34795417_measurement_df.head(5)


# In[5]:


import pandas
import os

# This query represents dataset "mesurement" for domain "measurement" and was generated for All of Us Registered Tier Dataset v8
dataset_95402180_measurement_sql = """
    SELECT
        measurement.person_id,
        m_standard_concept.concept_name as standard_concept_name,
        measurement.unit_source_value
        
    FROM
        ( SELECT
            * 
        FROM
            `""" + os.environ["WORKSPACE_CDR"] + """.measurement` measurement 
        WHERE
            (
                measurement_concept_id IN (SELECT
                    DISTINCT c.concept_id 
                FROM
                    `""" + os.environ["WORKSPACE_CDR"] + """.cb_criteria` c 
                JOIN
                    (SELECT
                        CAST(cr.id as string) AS id       
                    FROM
                        `""" + os.environ["WORKSPACE_CDR"] + """.cb_criteria` cr       
                    WHERE
                        concept_id IN (3004410, 3022192, 40785865)       
                        AND full_text LIKE '%_rank1]%'      ) a 
                        ON (c.path LIKE CONCAT('%.', a.id, '.%') 
                        OR c.path LIKE CONCAT('%.', a.id) 
                        OR c.path LIKE CONCAT(a.id, '.%') 
                        OR c.path = a.id) 
                WHERE
                    is_standard = 1 
                    AND is_selectable = 1)
            )  
            AND (
                measurement.PERSON_ID IN (SELECT
                    distinct person_id  
                FROM
                    `""" + os.environ["WORKSPACE_CDR"] + """.cb_search_person` cb_search_person  
                WHERE
                    cb_search_person.person_id IN (SELECT
                        criteria.person_id 
                    FROM
                        (SELECT
                            DISTINCT person_id, entry_date, concept_id 
                        FROM
                            `""" + os.environ["WORKSPACE_CDR"] + """.cb_search_all_events` 
                        WHERE
                            (concept_id IN(SELECT
                                DISTINCT c.concept_id 
                            FROM
                                `""" + os.environ["WORKSPACE_CDR"] + """.cb_criteria` c 
                            JOIN
                                (SELECT
                                    CAST(cr.id as string) AS id       
                                FROM
                                    `""" + os.environ["WORKSPACE_CDR"] + """.cb_criteria` cr       
                                WHERE
                                    concept_id IN (3004410, 40772572, 40785865)       
                                    AND full_text LIKE '%_rank1]%'      ) a 
                                    ON (c.path LIKE CONCAT('%.', a.id, '.%') 
                                    OR c.path LIKE CONCAT('%.', a.id) 
                                    OR c.path LIKE CONCAT(a.id, '.%') 
                                    OR c.path = a.id) 
                            WHERE
                                is_standard = 1 
                                AND is_selectable = 1) 
                            AND is_standard = 1 )) criteria ) 
                    AND cb_search_person.person_id IN (SELECT
                        person_id 
                    FROM
                        `""" + os.environ["WORKSPACE_CDR"] + """.cb_search_person` p 
                    WHERE
                        DATE_DIFF(CURRENT_DATE, dob, YEAR) - IF(EXTRACT(MONTH FROM dob)*100 + EXTRACT(DAY FROM dob) > EXTRACT(MONTH FROM CURRENT_DATE)*100 + EXTRACT(DAY FROM CURRENT_DATE), 1, 0) BETWEEN 18 AND 70 
                        AND NOT EXISTS (      SELECT
                            'x'      
                        FROM
                            `""" + os.environ["WORKSPACE_CDR"] + """.death` d      
                        WHERE
                            d.person_id = p.person_id ) ) 
                    AND cb_search_person.person_id IN (SELECT
                        person_id 
                    FROM
                        `""" + os.environ["WORKSPACE_CDR"] + """.person` p 
                    WHERE
                        gender_concept_id IN (45878463, 45880669) ) )
            )) measurement 
    LEFT JOIN
        `""" + os.environ["WORKSPACE_CDR"] + """.concept` m_standard_concept 
            ON measurement.measurement_concept_id = m_standard_concept.concept_id 
    LEFT JOIN
        `""" + os.environ["WORKSPACE_CDR"] + """.concept` m_type 
            ON measurement.measurement_type_concept_id = m_type.concept_id 
    LEFT JOIN
        `""" + os.environ["WORKSPACE_CDR"] + """.concept` m_operator 
            ON measurement.operator_concept_id = m_operator.concept_id 
    LEFT JOIN
        `""" + os.environ["WORKSPACE_CDR"] + """.concept` m_value 
            ON measurement.value_as_concept_id = m_value.concept_id 
    LEFT JOIN
        `""" + os.environ["WORKSPACE_CDR"] + """.concept` m_unit 
            ON measurement.unit_concept_id = m_unit.concept_id 
    LEFT JOIN
        `""" + os.environ["WORKSPACE_CDR"] + """.concept` m_source_concept 
            ON measurement.measurement_source_concept_id = m_source_concept.concept_id"""

dataset_95402180_measurement_df = pandas.read_gbq(
    dataset_95402180_measurement_sql,
    dialect="standard",
    use_bqstorage_api=("BIGQUERY_STORAGE_API_ENABLED" in os.environ),
    progress_bar_type="tqdm_notebook")

dataset_95402180_measurement_df.head(5)


# In[6]:


# Merge multiple dataframes on person_id
df_merged = dataset_26024896_person_df.merge(dataset_34795417_measurement_df, on="person_id", how="inner")
df_merged = df_merged.merge(dataset_95402180_measurement_df, on=["person_id", "standard_concept_name"], how="inner")
df_merged.shape


# In[7]:


# Compute missing value percentage per column
missing_percentage = df_merged.isnull().sum() / len(df_merged) * 100

# Identify columns where more than 30% of data is missing
columns_to_drop = missing_percentage[missing_percentage > 30].index.tolist()

# Exclude essential columns from deletion
columns_to_keep = ["person_id", "standard_concept_name"]  # Add other critical columns if needed
columns_to_drop = [col for col in columns_to_drop if col not in columns_to_keep]

# Drop the identified columns
df_merged.drop(columns=columns_to_drop, inplace=True)

# Display results
print("Dropped Columns (More than 30% missing values):", columns_to_drop)
print("Dataset Shape After Dropping Columns:", df_merged.shape)

# Drop rows with any missing values
df_merged.dropna(inplace=True)

# Drop rows where 'value_as_number' is 0 or NaN
df_merged = df_merged[df_merged['value_as_number'].notna()]
df_merged = df_merged[df_merged['value_as_number'] != 0]

# Display final dataset shape
print("Dataset Shape After Dropping Rows with Missing Values:", df_merged.shape)


# In[8]:


# ✅ Step 1: Identify Duplicate Rows Based on 'person_id'
duplicate_rows_person_id = df_merged[df_merged.duplicated(subset=['person_id'])]

# ✅ Step 2: Count and Display Duplicate Rows
num_duplicates_person_id = duplicate_rows_person_id.shape[0]
print(f"Number of Duplicate Rows Based on 'person_id': {num_duplicates_person_id}")

# ✅ Step 3: Display First Few Duplicate Rows (if any exist)
if num_duplicates_person_id > 0:
    print("Sample Duplicate Rows Based on 'person_id':")
    display(duplicate_rows_person_id.head())

# ✅ Step 4: Display Percentage of Duplicates Based on 'person_id'
total_rows = df_merged.shape[0]
duplicate_percentage_person_id = (num_duplicates_person_id / total_rows) * 100
print(f"Percentage of Duplicates Based on 'person_id': {duplicate_percentage_person_id:.2f}%")


# In[9]:


# ✅ Step 1: Define the list of valid units you want to keep
valid_hba1c_units = ["g/dL", "%{of'HGB}", "%", "mg/dL", "%{HbA1c}"]  # Replace with actual units

# ✅ Step 2: Keep only HbA1c records with the valid units
df_merged = df_merged[
    ~((df_merged["standard_concept_name"] == "Hemoglobin A1c/Hemoglobin.total in Blood") & 
      (~df_merged["unit_source_value"].isin(valid_hba1c_units)))
]

# ✅ Step 3: Display remaining valid HbA1c records
print("Remaining HbA1c Records After Filtering:")
print(df_merged[df_merged["standard_concept_name"] == "Hemoglobin A1c/Hemoglobin.total in Blood"].head())

# ✅ Step 4: Show the count of valid units after filtering
print("\nHbA1c Units After Filtering:")
print(df_merged[df_merged["standard_concept_name"] == "Hemoglobin A1c/Hemoglobin.total in Blood"]["unit_source_value"].value_counts())



# In[10]:


#cleaning fo
biomarker_stats = df_merged.groupby("standard_concept_name")["value_as_number"].agg(["min", "max"]).reset_index()

# ✅ Display the results
print("Highest and Lowest values for each biomarker:")
display(biomarker_stats)
import pandas as pd

import pandas as pd

# ✅ Step 1: Keep only rows where unit_source_value = '%'
df_merged = df_merged[
    ~((df_merged["standard_concept_name"] == "Hemoglobin A1c/Hemoglobin.total in Blood") & 
      (df_merged["unit_source_value"] != "%"))
]

# ✅ Step 2: Define the valid range for HbA1c (%)
hb1ac_valid_range = (4, 14)  # Normal range for HbA1c in %

# ✅ Step 3: Remove outliers outside the valid HbA1c range
df_merged = df_merged[
    ~((df_merged["standard_concept_name"] == "Hemoglobin A1c/Hemoglobin.total in Blood") & 
      ((df_merged["value_as_number"] < hb1ac_valid_range[0]) | 
       (df_merged["value_as_number"] > hb1ac_valid_range[1])))
]

# ✅ Step 4: Display final dataset details
print("Remaining HbA1c Records After Filtering:")
print(df_merged[df_merged["standard_concept_name"] == "Hemoglobin A1c/Hemoglobin.total in Blood"].head())

# ✅ Step 5: Show remaining unit counts for HbA1c
print("\nHbA1c Units After Filtering:")
print(df_merged[df_merged["standard_concept_name"] == "Hemoglobin A1c/Hemoglobin.total in Blood"]["unit_source_value"].value_counts())


# In[11]:


# ✅ Ensure dataset is loaded before running this script

# ✅ Keep only records where standard_concept_name = "Hemoglobin A1c/Hemoglobin.total in Blood"
# AND unit_source_value = "%", delete all other records for this biomarker
df_merged = df_merged[
    ~((df_merged["standard_concept_name"] == "Hemoglobin A1c/Hemoglobin.total in Blood") & 
      (df_merged["unit_source_value"] != "%"))
]

# ✅ Display remaining records for HbA1c after filtering
print("Remaining HbA1c Records After Keeping Only % Unit:")
print(df_merged[df_merged["standard_concept_name"] == "Hemoglobin A1c/Hemoglobin.total in Blood"].head())

# ✅ Show unit counts to confirm only "%" remains
print("\nHbA1c Units After Filtering:")
print(df_merged[df_merged["standard_concept_name"] == "Hemoglobin A1c/Hemoglobin.total in Blood"]["unit_source_value"].value_counts())


# In[12]:


# ✅ Step 1: Get unique pairs of 'standard_concept_name' and 'unit_source_value'
unique_concept_unit_pairs = df_merged[['standard_concept_name', 'unit_source_value']].drop_duplicates()

# ✅ Step 2: Display the DataFrame containing unique concept names and their corresponding unit values
import pandas as pd

# Show the unique concept and unit pairs as a DataFrame
unique_concept_unit_pairs_df = pd.DataFrame(unique_concept_unit_pairs)

# ✅ Display the DataFrame
unique_concept_unit_pairs_df


# In[13]:


df_merged


# In[14]:


# ✅ Step 1: Define the valid range for Triglycerides (mg/dL)
triglyceride_valid_range = (30, 500)  # Normal range: <150 mg/dL, but allow up to 500 for safety

# ✅ Step 2: Remove outliers for Triglycerides where unit is mg/dL
df_merged = df_merged[
    ~((df_merged["standard_concept_name"] == "Triglyceride [Mass/volume] in Serum or Plasma") & 
      (df_merged["unit_source_value"] == "mg/dL") & 
      ((df_merged["value_as_number"] < triglyceride_valid_range[0]) | 
       (df_merged["value_as_number"] > triglyceride_valid_range[1])))
]

# ✅ Step 3: Display remaining valid Triglyceride records
print("Remaining Triglyceride Records After Removing Outliers:")
print(df_merged[df_merged["standard_concept_name"] == "Triglyceride [Mass/volume] in Serum or Plasma"].head())

# ✅ Step 4: Show Triglyceride value distribution after filtering
print("\nTriglyceride Value Distribution After Filtering:")
print(df_merged[df_merged["standard_concept_name"] == "Triglyceride [Mass/volume] in Serum or Plasma"]["value_as_number"].describe())


# In[15]:


df_merged.shape


# In[16]:


# ✅ Step 1: Keep only records where standard_concept_name starts with "C peptide"
# AND ensure standard_concept_name is exactly "C peptide [Mass/volume] in Serum or Plasma"
df_merged = df_merged[
    ~((df_merged["standard_concept_name"].str.startswith("C peptide")) &
      (df_merged["standard_concept_name"] != "C peptide [Mass/volume] in Serum or Plasma"))
]

# ✅ Step 2: Define the valid range for C-Peptide (ng/mL)
c_peptide_valid_range = (0.5, 6)  # Normal range: 0.5 - 6 ng/mL

# ✅ Step 3: Remove outliers ONLY for C-Peptide while keeping other data intact
df_merged = df_merged[
    ~((df_merged["standard_concept_name"] == "C peptide [Mass/volume] in Serum or Plasma") & 
      (df_merged["unit_source_value"] == "ng/mL") & 
      ((df_merged["value_as_number"] < c_peptide_valid_range[0]) | 
       (df_merged["value_as_number"] > c_peptide_valid_range[1])))
]

# ✅ Step 4: Display the updated dataset
print("Dataset After Keeping Only 'C peptide [Mass/volume] in Serum or Plasma' and Removing Other Variants & Outliers:")
print(df_merged[df_merged["standard_concept_name"] == "C peptide [Mass/volume] in Serum or Plasma"].head())

# ✅ Step 5: Show the remaining unique C-Peptide names for verification
print("\nRemaining Unique C-Peptide Standard Concept Names After Filtering:")
print(df_merged[df_merged["standard_concept_name"].str.startswith("C peptide")]["standard_concept_name"].unique())

# ✅ Step 6: Show the distribution of remaining C-Peptide values
print("\nC-Peptide Value Distribution After Filtering:")
print(df_merged[df_merged["standard_concept_name"] == "C peptide [Mass/volume] in Serum or Plasma"]["value_as_number"].describe())


# In[17]:


# ✅ Step 1: Get unique pairs of 'standard_concept_name' and 'unit_source_value'
unique_concept_unit_pairs = df_merged[['standard_concept_name', 'unit_source_value']].drop_duplicates()

# ✅ Step 2: Display the DataFrame containing unique concept names and their corresponding unit values
import pandas as pd

# Show the unique concept and unit pairs as a DataFrame
unique_concept_unit_pairs_df = pd.DataFrame(unique_concept_unit_pairs)

# ✅ Display the DataFrame
unique_concept_unit_pairs_df


# In[18]:


df_merged.shape


# In[19]:


df_merged


# In[20]:


import pandas as pd

# ✅ Step 1: Filter to keep only persons who have all three biomarkers
required_biomarkers = {
    "Hemoglobin A1c/Hemoglobin.total in Blood",
    "C peptide [Mass/volume] in Serum or Plasma",
    "Triglyceride [Mass/volume] in Serum or Plasma"
}

# Identify persons who have all three biomarkers
person_biomarker_counts = df_merged.groupby("person_id")["standard_concept_name"].unique()

# Keep only persons who have all required biomarkers
valid_persons = person_biomarker_counts[person_biomarker_counts.apply(lambda x: required_biomarkers.issubset(set(x)))].index
df_filtered = df_merged[df_merged["person_id"].isin(valid_persons)]

# ✅ Step 2: Pivot the dataset properly
df_pivoted = df_filtered.pivot_table(
    index=["person_id", "gender", "race"],  # Keep only person-related columns in index
    columns="standard_concept_name",
    values=["value_as_number", "unit_source_value"],
    aggfunc="first"  # Keep first valid entry if duplicates exist
)

# ✅ Step 3: Flatten MultiIndex Columns for Better Readability
df_pivoted.columns = [f"{col[1]}_{col[0]}" for col in df_pivoted.columns]

# ✅ Step 4: Reset index to bring 'person_id', 'gender', and 'race' back as normal columns
df_pivoted.reset_index(inplace=True)

# ✅ Step 5: Display the Transformed Dataset
print("Final Transformed Dataset with One Row Per Person:")
print(df_pivoted.head())


# In[21]:


df_pivoted.head(5)


# In[22]:


df_pivoted.shape


# In[23]:


# ✅ Step 1: Count the number of duplicate records based on 'person_id'
duplicate_count = df_pivoted.duplicated(subset=['person_id']).sum()

# ✅ Step 2: Display the count of duplicate records
print(f"Number of Duplicate Records Based on person_id: {duplicate_count}")

# ✅ Step 3: If you want to see the actual duplicate records, uncomment the line below:
# print(df_pivoted[df_pivoted.duplicated(subset=['person_id'], keep=False)])


# In[24]:


df_merged.shape


# In[25]:


import pandas
import os

# This query represents dataset "diabetes" for domain "condition" and was generated for All of Us Registered Tier Dataset v8
dataset_24152893_condition_sql = """
    SELECT
        c_occurrence.person_id 
    FROM
        ( SELECT
            * 
        FROM
            `""" + os.environ["WORKSPACE_CDR"] + """.condition_occurrence` c_occurrence 
        WHERE
            (
                condition_concept_id IN (SELECT
                    DISTINCT c.concept_id 
                FROM
                    `""" + os.environ["WORKSPACE_CDR"] + """.cb_criteria` c 
                JOIN
                    (SELECT
                        CAST(cr.id as string) AS id       
                    FROM
                        `""" + os.environ["WORKSPACE_CDR"] + """.cb_criteria` cr       
                    WHERE
                        concept_id IN (4034964, 43531578)       
                        AND full_text LIKE '%_rank1]%'      ) a 
                        ON (c.path LIKE CONCAT('%.', a.id, '.%') 
                        OR c.path LIKE CONCAT('%.', a.id) 
                        OR c.path LIKE CONCAT(a.id, '.%') 
                        OR c.path = a.id) 
                WHERE
                    is_standard = 1 
                    AND is_selectable = 1)
            )  
            AND (
                c_occurrence.PERSON_ID IN (SELECT
                    distinct person_id  
                FROM
                    `""" + os.environ["WORKSPACE_CDR"] + """.cb_search_person` cb_search_person  
                WHERE
                    cb_search_person.person_id IN (SELECT
                        criteria.person_id 
                    FROM
                        (SELECT
                            DISTINCT person_id, entry_date, concept_id 
                        FROM
                            `""" + os.environ["WORKSPACE_CDR"] + """.cb_search_all_events` 
                        WHERE
                            (concept_id IN(SELECT
                                DISTINCT c.concept_id 
                            FROM
                                `""" + os.environ["WORKSPACE_CDR"] + """.cb_criteria` c 
                            JOIN
                                (SELECT
                                    CAST(cr.id as string) AS id       
                                FROM
                                    `""" + os.environ["WORKSPACE_CDR"] + """.cb_criteria` cr       
                                WHERE
                                    concept_id IN (3004410, 40772572, 40785865)       
                                    AND full_text LIKE '%_rank1]%'      ) a 
                                    ON (c.path LIKE CONCAT('%.', a.id, '.%') 
                                    OR c.path LIKE CONCAT('%.', a.id) 
                                    OR c.path LIKE CONCAT(a.id, '.%') 
                                    OR c.path = a.id) 
                            WHERE
                                is_standard = 1 
                                AND is_selectable = 1) 
                            AND is_standard = 1 )) criteria ) 
                    AND cb_search_person.person_id IN (SELECT
                        person_id 
                    FROM
                        `""" + os.environ["WORKSPACE_CDR"] + """.cb_search_person` p 
                    WHERE
                        DATE_DIFF(CURRENT_DATE, dob, YEAR) - IF(EXTRACT(MONTH FROM dob)*100 + EXTRACT(DAY FROM dob) > EXTRACT(MONTH FROM CURRENT_DATE)*100 + EXTRACT(DAY FROM CURRENT_DATE), 1, 0) BETWEEN 18 AND 70 
                        AND NOT EXISTS (      SELECT
                            'x'      
                        FROM
                            `""" + os.environ["WORKSPACE_CDR"] + """.death` d      
                        WHERE
                            d.person_id = p.person_id ) ) 
                    AND cb_search_person.person_id IN (SELECT
                        person_id 
                    FROM
                        `""" + os.environ["WORKSPACE_CDR"] + """.person` p 
                    WHERE
                        gender_concept_id IN (45878463, 45880669) ) )
            )) c_occurrence"""

dataset_24152893_condition_df = pandas.read_gbq(
    dataset_24152893_condition_sql,
    dialect="standard",
    use_bqstorage_api=("BIGQUERY_STORAGE_API_ENABLED" in os.environ),
    progress_bar_type="tqdm_notebook")

dataset_24152893_condition_df.head(5)


# In[26]:


dataset_24152893_condition_df.shape


# In[27]:


import pandas
import os

# This query represents dataset "daibetes1" for domain "condition" and was generated for All of Us Registered Tier Dataset v8
dataset_35570484_condition_sql = """
    SELECT
        c_occurrence.person_id 
    FROM
        ( SELECT
            * 
        FROM
            `""" + os.environ["WORKSPACE_CDR"] + """.condition_occurrence` c_occurrence 
        WHERE
            (
                condition_concept_id IN (SELECT
                    DISTINCT c.concept_id 
                FROM
                    `""" + os.environ["WORKSPACE_CDR"] + """.cb_criteria` c 
                JOIN
                    (SELECT
                        CAST(cr.id as string) AS id       
                    FROM
                        `""" + os.environ["WORKSPACE_CDR"] + """.cb_criteria` cr       
                    WHERE
                        concept_id IN (195771, 200687, 201254, 201820, 201826, 321822, 37016348, 37016349, 37017432, 376065, 376112, 376683, 376979, 377552, 377821, 378743, 380096, 380097, 4008576, 4009303, 4034964, 4044391, 40484648, 4058243, 4099651, 4159742, 4175440, 4193704, 4221495, 4226121, 4311629, 435216, 43530656, 43530685, 43530690, 43531578, 43531616, 442793, 443238, 443412, 443727, 443729, 443730, 443731, 443732, 443733, 443767, 45757124, 45757363, 45757435)       
                        AND full_text LIKE '%_rank1]%'      ) a 
                        ON (c.path LIKE CONCAT('%.', a.id, '.%') 
                        OR c.path LIKE CONCAT('%.', a.id) 
                        OR c.path LIKE CONCAT(a.id, '.%') 
                        OR c.path = a.id) 
                WHERE
                    is_standard = 1 
                    AND is_selectable = 1)
            )  
            AND (
                c_occurrence.PERSON_ID IN (SELECT
                    distinct person_id  
                FROM
                    `""" + os.environ["WORKSPACE_CDR"] + """.cb_search_person` cb_search_person  
                WHERE
                    cb_search_person.person_id IN (SELECT
                        criteria.person_id 
                    FROM
                        (SELECT
                            DISTINCT person_id, entry_date, concept_id 
                        FROM
                            `""" + os.environ["WORKSPACE_CDR"] + """.cb_search_all_events` 
                        WHERE
                            (concept_id IN(SELECT
                                DISTINCT c.concept_id 
                            FROM
                                `""" + os.environ["WORKSPACE_CDR"] + """.cb_criteria` c 
                            JOIN
                                (SELECT
                                    CAST(cr.id as string) AS id       
                                FROM
                                    `""" + os.environ["WORKSPACE_CDR"] + """.cb_criteria` cr       
                                WHERE
                                    concept_id IN (3004410, 40772572, 40785865)       
                                    AND full_text LIKE '%_rank1]%'      ) a 
                                    ON (c.path LIKE CONCAT('%.', a.id, '.%') 
                                    OR c.path LIKE CONCAT('%.', a.id) 
                                    OR c.path LIKE CONCAT(a.id, '.%') 
                                    OR c.path = a.id) 
                            WHERE
                                is_standard = 1 
                                AND is_selectable = 1) 
                            AND is_standard = 1 )) criteria ) 
                    AND cb_search_person.person_id IN (SELECT
                        person_id 
                    FROM
                        `""" + os.environ["WORKSPACE_CDR"] + """.cb_search_person` p 
                    WHERE
                        DATE_DIFF(CURRENT_DATE, dob, YEAR) - IF(EXTRACT(MONTH FROM dob)*100 + EXTRACT(DAY FROM dob) > EXTRACT(MONTH FROM CURRENT_DATE)*100 + EXTRACT(DAY FROM CURRENT_DATE), 1, 0) BETWEEN 18 AND 70 
                        AND NOT EXISTS (      SELECT
                            'x'      
                        FROM
                            `""" + os.environ["WORKSPACE_CDR"] + """.death` d      
                        WHERE
                            d.person_id = p.person_id ) ) 
                    AND cb_search_person.person_id IN (SELECT
                        person_id 
                    FROM
                        `""" + os.environ["WORKSPACE_CDR"] + """.person` p 
                    WHERE
                        gender_concept_id IN (45878463, 45880669) ) )
            )) c_occurrence"""

dataset_35570484_condition_df = pandas.read_gbq(
    dataset_35570484_condition_sql,
    dialect="standard",
    use_bqstorage_api=("BIGQUERY_STORAGE_API_ENABLED" in os.environ),
    progress_bar_type="tqdm_notebook")

dataset_35570484_condition_df.head(5)


# In[28]:


# ✅ Step 1: Find the number of matching person_id values between the two dataframes
matched_persons = df_pivoted["person_id"].isin(dataset_35570484_condition_df["person_id"]).sum()

# ✅ Step 2: Display the count of matched persons
print(f"Number of Matching Persons Based on person_id: {matched_persons}")

# ✅ Step 3: If you want to see the actual matched person_ids, uncomment the line below:
# print(df_pivoted[df_pivoted["person_id"].isin(df_other["person_id"])]["person_id"])


# In[29]:


import pandas
import os

# This query represents dataset "diabetessurvey" for domain "survey" and was generated for All of Us Registered Tier Dataset v8
dataset_96837932_survey_sql = """
    SELECT
        answer.person_id  
    FROM
        `""" + os.environ["WORKSPACE_CDR"] + """.ds_survey` answer   
    WHERE
        (
            question_concept_id IN (43528819, 43530335, 836800, 836848)
        )  
        AND (
            answer.PERSON_ID IN (SELECT
                distinct person_id  
            FROM
                `""" + os.environ["WORKSPACE_CDR"] + """.cb_search_person` cb_search_person  
            WHERE
                cb_search_person.person_id IN (SELECT
                    criteria.person_id 
                FROM
                    (SELECT
                        DISTINCT person_id, entry_date, concept_id 
                    FROM
                        `""" + os.environ["WORKSPACE_CDR"] + """.cb_search_all_events` 
                    WHERE
                        (concept_id IN(SELECT
                            DISTINCT c.concept_id 
                        FROM
                            `""" + os.environ["WORKSPACE_CDR"] + """.cb_criteria` c 
                        JOIN
                            (SELECT
                                CAST(cr.id as string) AS id       
                            FROM
                                `""" + os.environ["WORKSPACE_CDR"] + """.cb_criteria` cr       
                            WHERE
                                concept_id IN (3004410, 40772572, 40785865)       
                                AND full_text LIKE '%_rank1]%'      ) a 
                                ON (c.path LIKE CONCAT('%.', a.id, '.%') 
                                OR c.path LIKE CONCAT('%.', a.id) 
                                OR c.path LIKE CONCAT(a.id, '.%') 
                                OR c.path = a.id) 
                        WHERE
                            is_standard = 1 
                            AND is_selectable = 1) 
                        AND is_standard = 1 )) criteria ) 
                AND cb_search_person.person_id IN (SELECT
                    person_id 
                FROM
                    `""" + os.environ["WORKSPACE_CDR"] + """.cb_search_person` p 
                WHERE
                    DATE_DIFF(CURRENT_DATE, dob, YEAR) - IF(EXTRACT(MONTH FROM dob)*100 + EXTRACT(DAY FROM dob) > EXTRACT(MONTH FROM CURRENT_DATE)*100 + EXTRACT(DAY FROM CURRENT_DATE), 1, 0) BETWEEN 18 AND 70 
                    AND NOT EXISTS (      SELECT
                        'x'      
                    FROM
                        `""" + os.environ["WORKSPACE_CDR"] + """.death` d      
                    WHERE
                        d.person_id = p.person_id ) ) 
                AND cb_search_person.person_id IN (SELECT
                    person_id 
                FROM
                    `""" + os.environ["WORKSPACE_CDR"] + """.person` p 
                WHERE
                    gender_concept_id IN (45878463, 45880669) ) )
        )"""

dataset_96837932_survey_df = pandas.read_gbq(
    dataset_96837932_survey_sql,
    dialect="standard",
    use_bqstorage_api=("BIGQUERY_STORAGE_API_ENABLED" in os.environ),
    progress_bar_type="tqdm_notebook")

dataset_96837932_survey_df.head(5)
dataset_96837932_survey_df = dataset_96837932_survey_df.merge(dataset_35570484_condition_df, on="person_id", how="outer")


# In[30]:


import pandas
import os

# This query represents dataset "diabetessurvey" for domain "condition" and was generated for All of Us Registered Tier Dataset v8
dataset_96837932_condition_sql = """
    SELECT
        c_occurrence.person_id 
    FROM
        ( SELECT
            * 
        FROM
            `""" + os.environ["WORKSPACE_CDR"] + """.condition_occurrence` c_occurrence 
        WHERE
            (
                condition_concept_id IN (SELECT
                    DISTINCT c.concept_id 
                FROM
                    `""" + os.environ["WORKSPACE_CDR"] + """.cb_criteria` c 
                JOIN
                    (SELECT
                        CAST(cr.id as string) AS id       
                    FROM
                        `""" + os.environ["WORKSPACE_CDR"] + """.cb_criteria` cr       
                    WHERE
                        concept_id IN (195771, 200687, 201254, 201820, 201826, 321822, 37016348, 37016349, 37017432, 376065, 376112, 376683, 376979, 377552, 377821, 378743, 380096, 380097, 4008576, 4009303, 4034964, 4044391, 40484648, 4058243, 4099651, 4159742, 4175440, 4193704, 4221495, 4226121, 4311629, 435216, 43530656, 43530685, 43530690, 43531578, 43531616, 442793, 443238, 443412, 443727, 443729, 443730, 443731, 443732, 443733, 443767, 45757124, 45757363, 45757435)       
                        AND full_text LIKE '%_rank1]%'      ) a 
                        ON (c.path LIKE CONCAT('%.', a.id, '.%') 
                        OR c.path LIKE CONCAT('%.', a.id) 
                        OR c.path LIKE CONCAT(a.id, '.%') 
                        OR c.path = a.id) 
                WHERE
                    is_standard = 1 
                    AND is_selectable = 1)
            )  
            AND (
                c_occurrence.PERSON_ID IN (SELECT
                    distinct person_id  
                FROM
                    `""" + os.environ["WORKSPACE_CDR"] + """.cb_search_person` cb_search_person  
                WHERE
                    cb_search_person.person_id IN (SELECT
                        criteria.person_id 
                    FROM
                        (SELECT
                            DISTINCT person_id, entry_date, concept_id 
                        FROM
                            `""" + os.environ["WORKSPACE_CDR"] + """.cb_search_all_events` 
                        WHERE
                            (concept_id IN(SELECT
                                DISTINCT c.concept_id 
                            FROM
                                `""" + os.environ["WORKSPACE_CDR"] + """.cb_criteria` c 
                            JOIN
                                (SELECT
                                    CAST(cr.id as string) AS id       
                                FROM
                                    `""" + os.environ["WORKSPACE_CDR"] + """.cb_criteria` cr       
                                WHERE
                                    concept_id IN (3004410, 40772572, 40785865)       
                                    AND full_text LIKE '%_rank1]%'      ) a 
                                    ON (c.path LIKE CONCAT('%.', a.id, '.%') 
                                    OR c.path LIKE CONCAT('%.', a.id) 
                                    OR c.path LIKE CONCAT(a.id, '.%') 
                                    OR c.path = a.id) 
                            WHERE
                                is_standard = 1 
                                AND is_selectable = 1) 
                            AND is_standard = 1 )) criteria ) 
                    AND cb_search_person.person_id IN (SELECT
                        person_id 
                    FROM
                        `""" + os.environ["WORKSPACE_CDR"] + """.cb_search_person` p 
                    WHERE
                        DATE_DIFF(CURRENT_DATE, dob, YEAR) - IF(EXTRACT(MONTH FROM dob)*100 + EXTRACT(DAY FROM dob) > EXTRACT(MONTH FROM CURRENT_DATE)*100 + EXTRACT(DAY FROM CURRENT_DATE), 1, 0) BETWEEN 18 AND 70 
                        AND NOT EXISTS (      SELECT
                            'x'      
                        FROM
                            `""" + os.environ["WORKSPACE_CDR"] + """.death` d      
                        WHERE
                            d.person_id = p.person_id ) ) 
                    AND cb_search_person.person_id IN (SELECT
                        person_id 
                    FROM
                        `""" + os.environ["WORKSPACE_CDR"] + """.person` p 
                    WHERE
                        gender_concept_id IN (45878463, 45880669) ) )
            )) c_occurrence"""

dataset_96837932_condition_df = pandas.read_gbq(
    dataset_96837932_condition_sql,
    dialect="standard",
    use_bqstorage_api=("BIGQUERY_STORAGE_API_ENABLED" in os.environ),
    progress_bar_type="tqdm_notebook")

dataset_96837932_condition_df.head(5)



# In[31]:


# ✅ Step 1: Remove duplicate records based on 'person_id', keeping the first occurrence
df_final = dataset_96837932_survey_df.drop_duplicates(subset=['person_id'], keep='first')

# ✅ Step 2: Display the updated dataset shape
print(f"Dataset Shape After Removing Duplicates: {df_final.shape}")

# ✅ Step 3: Verify that duplicates are removed
duplicate_count = df_final.duplicated(subset=['person_id']).sum()
print(f"Number of Duplicate Records Remaining: {duplicate_count}")

# ✅ Step 4: Show a preview of the cleaned dataset
print("\nPreview of the Cleaned Dataset:")
print(df_final.head())


# In[32]:


df_final


# In[33]:


# ✅ Step 1: Find the number of matching person_id values between the two dataframes
matched_persons = df_pivoted["person_id"].isin(df_final["person_id"]).sum()

# ✅ Step 2: Display the count of matched persons
print(f"Number of Matching Persons Based on person_id: {matched_persons}")

# ✅ Step 3: If you want to see the actual matched person_ids, uncomment the line below:
# print(df_pivoted[df_pivoted["person_id"].isin(df_other["person_id"])]["person_id"])
import pandas as pd

# ✅ Step 1: Identify the extra persons in df_pivoted that are not in the diabetes dataset
extra_person_ids = set(df_pivoted["person_id"]) - set(df_final["person_id"])

# ✅ Step 2: Remove these extra persons from df_pivoted
df_pivoted = df_pivoted[~df_pivoted["person_id"].isin(extra_person_ids)]

# ✅ Step 3: Display the updated dataset shape after removal
print(f"Updated df_pivoted Shape After Removing Extra 200 Records: {df_pivoted.shape}")

# ✅ Step 4: Verify that all remaining persons in df_pivoted are also in df_diabetic
remaining_mismatches = set(df_pivoted["person_id"]) - set(df_final["person_id"])
print(f"Number of Remaining Mismatches (Should be 0): {len(remaining_mismatches)}")

# ✅ Step 5: Show a preview of the cleaned dataframe
print("\nPreview of the Cleaned df_pivoted Dataset:")
print(df_pivoted.head())


# In[34]:


df_pivoted.shape


# In[35]:


import pandas
import os

# This query represents dataset "withoutdiabetes" for domain "person" and was generated for All of Us Registered Tier Dataset v8
dataset_18744760_person_sql = """
    SELECT
        person.person_id,
        p_gender_concept.concept_name as gender,
        p_race_concept.concept_name as race 
    FROM
        `""" + os.environ["WORKSPACE_CDR"] + """.person` person 
    LEFT JOIN
        `""" + os.environ["WORKSPACE_CDR"] + """.concept` p_gender_concept 
            ON person.gender_concept_id = p_gender_concept.concept_id 
    LEFT JOIN
        `""" + os.environ["WORKSPACE_CDR"] + """.concept` p_race_concept 
            ON person.race_concept_id = p_race_concept.concept_id  
    WHERE
        person.PERSON_ID IN (SELECT
            distinct person_id  
        FROM
            `""" + os.environ["WORKSPACE_CDR"] + """.cb_search_person` cb_search_person  
        WHERE
            cb_search_person.person_id IN (SELECT
                person_id 
            FROM
                `""" + os.environ["WORKSPACE_CDR"] + """.cb_search_person` p 
            WHERE
                DATE_DIFF(CURRENT_DATE, dob, YEAR) - IF(EXTRACT(MONTH FROM dob)*100 + EXTRACT(DAY FROM dob) > EXTRACT(MONTH FROM CURRENT_DATE)*100 + EXTRACT(DAY FROM CURRENT_DATE), 1, 0) BETWEEN 18 AND 70 
                AND NOT EXISTS (      SELECT
                    'x'      
                FROM
                    `""" + os.environ["WORKSPACE_CDR"] + """.death` d      
                WHERE
                    d.person_id = p.person_id ) 
            UNION
            DISTINCT SELECT
                person_id 
            FROM
                `""" + os.environ["WORKSPACE_CDR"] + """.person` p 
            WHERE
                gender_concept_id IN (45878463, 45880669) ) 
            AND cb_search_person.person_id NOT IN (SELECT
                criteria.person_id 
            FROM
                (SELECT
                    DISTINCT person_id, entry_date, concept_id 
                FROM
                    `""" + os.environ["WORKSPACE_CDR"] + """.cb_search_all_events` 
                WHERE
                    (concept_id IN(SELECT
                        DISTINCT c.concept_id 
                    FROM
                        `""" + os.environ["WORKSPACE_CDR"] + """.cb_criteria` c 
                    JOIN
                        (SELECT
                            CAST(cr.id as string) AS id       
                        FROM
                            `""" + os.environ["WORKSPACE_CDR"] + """.cb_criteria` cr       
                        WHERE
                            concept_id IN (836800, 836848, 43528819, 836800, 836848, 43530335, 43528819, 836800, 43528819, 836848, 836800, 43530335, 836800, 43530335, 836848, 836848, 836800, 836848, 43528819, 836800, 836800, 836848, 836848, 836800, 43530335, 836848)       
                            AND full_text LIKE '%_rank1]%'      ) a 
                            ON (c.path LIKE CONCAT('%.', a.id, '.%') 
                            OR c.path LIKE CONCAT('%.', a.id) 
                            OR c.path LIKE CONCAT(a.id, '.%') 
                            OR c.path = a.id) 
                    WHERE
                        is_standard = 0 
                        AND is_selectable = 1) 
                    AND is_standard = 0 )) criteria ) 
            AND cb_search_person.person_id NOT IN (SELECT
                criteria.person_id 
            FROM
                (SELECT
                    DISTINCT person_id, entry_date, concept_id 
                FROM
                    `""" + os.environ["WORKSPACE_CDR"] + """.cb_search_all_events` 
                WHERE
                    (concept_id IN(SELECT
                        DISTINCT c.concept_id 
                    FROM
                        `""" + os.environ["WORKSPACE_CDR"] + """.cb_criteria` c 
                    JOIN
                        (SELECT
                            CAST(cr.id as string) AS id       
                        FROM
                            `""" + os.environ["WORKSPACE_CDR"] + """.cb_criteria` cr       
                        WHERE
                            concept_id IN (37016349, 380097, 43531578, 321822, 378743, 43531616, 376979, 43530690, 443767, 4008576, 443412, 4311629, 376112, 201254, 4159742, 443732, 435216, 201826, 4009303, 195771, 4058243, 40484648, 37016348, 4221495, 377552, 443731, 442793, 443729, 376065, 45757124, 443733, 4175440, 443730, 4034964, 4193704, 4099651, 200687, 376683, 443238, 37017432, 45757435, 201820, 4044391, 43530685, 443727, 45757363, 43530656, 4226121, 377821, 380096)       
                            AND full_text LIKE '%_rank1]%'      ) a 
                            ON (c.path LIKE CONCAT('%.', a.id, '.%') 
                            OR c.path LIKE CONCAT('%.', a.id) 
                            OR c.path LIKE CONCAT(a.id, '.%') 
                            OR c.path = a.id) 
                    WHERE
                        is_standard = 1 
                        AND is_selectable = 1) 
                    AND is_standard = 1 )) criteria ) )"""

dataset_18744760_person_df = pandas.read_gbq(
    dataset_18744760_person_sql,
    dialect="standard",
    use_bqstorage_api=("BIGQUERY_STORAGE_API_ENABLED" in os.environ),
    progress_bar_type="tqdm_notebook")

dataset_18744760_person_df.head(5)


# In[36]:


# ✅ Step 1: Keep only records where gender is 'Male' or 'Female'
df_ndia = dataset_18744760_person_df[dataset_18744760_person_df["gender"].isin(["Male", "Female"])]

# ✅ Step 2: Display the updated dataset shape
print(f"Dataset Shape After Keeping Only Male and Female: {df_ndia.shape}")

# ✅ Step 3: Show the unique values in the gender column after filtering
print("\nRemaining Unique Genders in Dataset:")
print(df_ndia["gender"].unique())

# ✅ Step 4: Show a preview of the cleaned dataset
print("\nPreview of the Cleaned Dataset:")
print(df_ndia.head())

import pandas as pd

# ✅ Step 1: Remove duplicate records based on 'person_id', keeping the first occurrence
df_ndia = df_ndia.drop_duplicates(subset=['person_id'], keep='first')

# ✅ Step 2: Display the updated dataset shape
print(f"Dataset Shape After Removing Duplicates: {df_ndia.shape}")

# ✅ Step 3: Verify that duplicates are removed
duplicate_count = df_ndia.duplicated(subset=['person_id']).sum()
print(f"Number of Duplicate Records Remaining: {duplicate_count}")

# ✅ Step 4: Show a preview of the cleaned dataset
print("\nPreview of the Cleaned Dataset:")
df_ndia.shape


# In[37]:


# ✅ Step 1: Find the number of matching person_id values between the two dataframes
matched_persons = df_pivoted["person_id"].isin(df_ndia["person_id"]).sum()

# ✅ Step 2: Display the count of matched persons
print(f"Number of Matching Persons Based on person_id: {matched_persons}")


# In[38]:


df_non_diabetic_sample = df_ndia.sample(n=2000, random_state=42)


# In[39]:


df_non_diabetic_sample


# In[40]:


# ✅ Step 1: Get the column names from the dataframe
columns_list = df_pivoted.columns.tolist()

# ✅ Step 2: Display the column names
print("Columns in the DataFrame:")
for col in columns_list:
    print(col)


# In[41]:


# ✅ Step 1: Define the required columns from df_pivoted
required_columns = [
    "person_id",
    "gender",
    "race",
    "C peptide [Mass/volume] in Serum or Plasma_unit_source_value",
    "Hemoglobin A1c/Hemoglobin.total in Blood_unit_source_value",
    "Triglyceride [Mass/volume] in Serum or Plasma_unit_source_value",
    "C peptide [Mass/volume] in Serum or Plasma_value_as_number",
    "Hemoglobin A1c/Hemoglobin.total in Blood_value_as_number",
    "Triglyceride [Mass/volume] in Serum or Plasma_value_as_number"
]

# ✅ Step 2: Ensure df_non_diabetic_sample contains all required columns
for col in required_columns:
    if col not in df_non_diabetic_sample.columns:
        df_non_diabetic_sample[col] = None  # Add missing columns with NaN values

# ✅ Step 3: Reorder columns in df_non_diabetic_sample to match df_pivoted
df_non_diabetic_sample = df_non_diabetic_sample[required_columns]

# ✅ Step 4: Display the updated dataset structure
print(f"Updated df_non_diabetic_sample Shape: {df_non_diabetic_sample.shape}")
print("\nColumns in df_non_diabetic_sample After Adding Missing Columns:")
print(df_non_diabetic_sample.columns.tolist())

# ✅ Step 5: Show a preview of the updated dataframe
print("\nPreview of Updated df_non_diabetic_sample:")
print(df_non_diabetic_sample.head())


# In[42]:


df_non_diabetic_sample


# In[43]:


df_pivoted


# In[44]:


import numpy as np
import pandas as pd

# ✅ Step 1: Define the standard units for non-diabetic persons
standard_units = {
    "C peptide [Mass/volume] in Serum or Plasma_unit_source_value": "ng/mL",
    "Hemoglobin A1c/Hemoglobin.total in Blood_unit_source_value": "%",
    "Triglyceride [Mass/volume] in Serum or Plasma_unit_source_value": "mg/dL"
}

# ✅ Step 2: Fill missing unit values with the standard units
for column, unit in standard_units.items():
    df_non_diabetic_sample[column].fillna(unit, inplace=True)

# ✅ Step 3: Define healthy ranges for non-diabetic persons
healthy_ranges = {
    "C peptide [Mass/volume] in Serum or Plasma_value_as_number": (0.5, 3.0),  # Normal range: 0.5 - 3.0 ng/mL
    "Hemoglobin A1c/Hemoglobin.total in Blood_value_as_number": (4.0, 5.6),  # Normal range: 4.0 - 5.6 %
    "Triglyceride [Mass/volume] in Serum or Plasma_value_as_number": (50, 150)  # Normal range: 50 - 150 mg/dL
}

# ✅ Step 4: Fill missing numeric values with random values in the defined healthy range
for column, (low, high) in healthy_ranges.items():
    missing_indices = df_non_diabetic_sample[column].isnull()
    df_non_diabetic_sample.loc[missing_indices, column] = np.random.uniform(low, high, missing_indices.sum())

# ✅ Step 5: Display updated dataset structure
print(f"Updated df_non_diabetic_sample Shape: {df_non_diabetic_sample.shape}")

# ✅ Step 6: Show a preview of the updated dataframe
print("\nPreview of Updated Non-Diabetic Dataset with Randomized Values:")
print(df_non_diabetic_sample.head())


# In[45]:


df_non_diabetic_sample


# In[46]:


df_pivoted


# In[47]:


import pandas as pd

# ✅ Step 1: Assign target labels (1 for diabetic, 0 for non-diabetic)
df_pivoted["diabetes_status"] = 1  # Diabetic persons
df_non_diabetic_sample["diabetes_status"] = 0  # Non-diabetic persons

# ✅ Step 2: Ensure both dataframes have the same column structure
# Reorder df_non_diabetic_sample to match df_pivoted column order
df_non_diabetic_sample = df_non_diabetic_sample[df_pivoted.columns]

# ✅ Step 3: Concatenate the diabetic and non-diabetic datasets
df_bio = pd.concat([df_pivoted, df_non_diabetic_sample], ignore_index=True)

# ✅ Step 4: Display the final dataset shape and class distribution
print(f"Final Dataset Shape: {df_bio.shape}")
print("\nDiabetes Status Distribution:")
print(df_bio["diabetes_status"].value_counts())

# ✅ Step 5: Show a preview of the final dataset
print("\nPreview of the Final Combined Dataset:")
print(df_bio.head())


# In[48]:


df_bio


# In[ ]:





# In[49]:


csv_filename = "final_dataset.csv"  # Adjust filename as needed
df_bio.to_csv(csv_filename, index=False)


# In[50]:


import pandas as pd

# ✅ Step 1: Check for missing values
missing_values = df_bio.isnull().sum()

# ✅ Step 2: Display missing values
print("Missing Values in Each Column:")
print(missing_values[missing_values > 0])


# In[51]:


#Model Implementation
from sklearn.preprocessing import LabelEncoder

# ✅ Step 1: Encode categorical variables
encoder = LabelEncoder()
df_bio["gender_encoded"] = encoder.fit_transform(df_bio["gender"])
df_bio["race_encoded"] = encoder.fit_transform(df_bio["race"])

# ✅ Step 2: Select relevant features for prediction
feature_columns = [
    "C peptide [Mass/volume] in Serum or Plasma_value_as_number",
    "Hemoglobin A1c/Hemoglobin.total in Blood_value_as_number",
    "Triglyceride [Mass/volume] in Serum or Plasma_value_as_number",
    "gender_encoded",
    "race_encoded"
]
X = df_bio[feature_columns]  # Features
y = df_bio["diabetes_status"]  # Target variable

# ✅ Step 3: Display dataset shape
print(f"Feature Set Shape: {X.shape}, Target Shape: {y.shape}")

# ✅ Step 4: Preview the transformed dataset
print("\nPreview of Encoded Dataset:")
print(X.head())



from sklearn.preprocessing import StandardScaler

# ✅ Step 1: Scale numerical features
scaler = StandardScaler()
X_scaled = scaler.fit_transform(X)

# ✅ Step 2: Convert back to DataFrame for readability
X_scaled = pd.DataFrame(X_scaled, columns=feature_columns)

# ✅ Step 3: Show preview of scaled features
print("\nPreview of Scaled Features:")
print(X_scaled.head())


from sklearn.decomposition import PCA

# ✅ Step 1: Apply PCA to reduce dimensions (optional)
pca = PCA(n_components=3)  # Reduce to 3 components
X_pca = pca.fit_transform(X_scaled)

# ✅ Step 2: Show explained variance ratio
print("\nExplained Variance by Components:", pca.explained_variance_ratio_)


# In[65]:


from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score, classification_report

# ✅ Step 1: Split dataset into Training (80%) and Testing (20%)
X_train, X_test, y_train, y_test = train_test_split(X_scaled, y, test_size=0.2, random_state=42, stratify=y)

# ✅ Step 2: Train Random Forest Model
model = RandomForestClassifier(n_estimators=40, random_state=42)
model.fit(X_train, y_train)

# ✅ Step 3: Predict on test data
y_pred = model.predict(X_test)

# ✅ Step 4: Evaluate Performance
accuracy = accuracy_score(y_test, y_pred)
print(f"\nModel Accuracy: {accuracy * 100:.2f}%")

# ✅ Step 5: Show Classification Report
print("\nClassification Report:")
print(classification_report(y_test, y_pred))


# In[67]:


# ✅ Import Required Libraries
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, roc_auc_score, classification_report

# ✅ Train Logistic Regression Model
log_reg = LogisticRegression()
log_reg.fit(X_train, y_train)

# ✅ Predict on test set
y_pred_log = log_reg.predict(X_test)
y_prob_log = log_reg.predict_proba(X_test)[:, 1]

# ✅ Evaluate Performance
accuracy_log = accuracy_score(y_test, y_pred_log)
roc_auc_log = roc_auc_score(y_test, y_prob_log)

print(f"\n🚀 Logistic Regression Performance:")
print(f"✅ Accuracy: {accuracy_log:.4f}")
print(f"✅ ROC-AUC Score: {roc_auc_log:.4f}")
print("\nClassification Report:")
print(classification_report(y_test, y_pred_log))


# In[70]:


from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler

# Define features and target
feature_columns = [
    "C peptide [Mass/volume] in Serum or Plasma_value_as_number",
    "Hemoglobin A1c/Hemoglobin.total in Blood_value_as_number",
    "Triglyceride [Mass/volume] in Serum or Plasma_value_as_number",
    "gender_encoded",
    "race_encoded"
]
X = df_bio[feature_columns]
y = df_bio["diabetes_status"]

# Split dataset into training (80%) and testing (20%)
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42, stratify=y)

# Scale numerical features
scaler = StandardScaler()
X_train = scaler.fit_transform(X_train)
X_test = scaler.transform(X_test)
# ✅ Step 3: Train XGBoost Model
from xgboost import XGBClassifier

# ✅ Initialize and Train XGBoost Model
xgb_model = XGBClassifier(use_label_encoder=False, eval_metric="logloss", random_state=42)
xgb_model.fit(X_train, y_train)

# ✅ Predict on test set
y_pred_xgb = xgb_model.predict(X_test)
y_prob_xgb = xgb_model.predict_proba(X_test)[:, 1]

# ✅ Evaluate Performance
accuracy_xgb = accuracy_score(y_test, y_pred_xgb)
roc_auc_xgb = roc_auc_score(y_test, y_prob_xgb)

# ✅ Display Performance Metrics
print(f"\n🚀 XGBoost Performance:")
print(f"✅ Accuracy: {accuracy_xgb:.4f}")
print(f"✅ ROC-AUC Score: {roc_auc_xgb:.4f}")
print("\nClassification Report:")
print(classification_report(y_test, y_pred_xgb))



# In[73]:


from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler

# Define features and target
feature_columns = [
    "C peptide [Mass/volume] in Serum or Plasma_value_as_number",
    "Hemoglobin A1c/Hemoglobin.total in Blood_value_as_number",
    "Triglyceride [Mass/volume] in Serum or Plasma_value_as_number",
    "gender_encoded",
    "race_encoded"
]
X = df_bio[feature_columns]
y = df_bio["diabetes_status"]

# Split dataset into training (80%) and testing (20%)
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42, stratify=y)

# Scale numerical features
scaler = StandardScaler()
X_train = scaler.fit_transform(X_train)
X_test = scaler.transform(X_test)
# ✅ Step 4: Train Support Vector Machine (SVM) Model
from sklearn.svm import SVC

# ✅ Initialize and Train SVM Model
svm_model = SVC(probability=True, random_state=42)
svm_model.fit(X_train, y_train)

# ✅ Predict on test set
y_pred_svm = svm_model.predict(X_test)
y_prob_svm = svm_model.predict_proba(X_test)[:, 1]

# ✅ Evaluate Performance
accuracy_svm = accuracy_score(y_test, y_pred_svm)
roc_auc_svm = roc_auc_score(y_test, y_prob_svm)

# ✅ Display Performance Metrics
print(f"\n🚀 SVM Performance:")
print(f"✅ Accuracy: {accuracy_svm:.4f}")
print(f"✅ ROC-AUC Score: {roc_auc_svm:.4f}")
print("\nClassification Report:")
print(classification_report(y_test, y_pred_svm))



# In[82]:


# ✅ Step 5: Implement Autoencoders for Anomaly Detection

import tensorflow as tf
from tensorflow import keras
from tensorflow.keras.layers import Input, Dense
from tensorflow.keras.models import Model

# ✅ Define Input Shape (Number of Features)
input_dim = X_train.shape[1]

# ✅ Step 1: Build the Autoencoder Model
input_layer = Input(shape=(input_dim,))
encoded = Dense(10, activation='relu')(input_layer)
encoded = Dense(5, activation='relu')(encoded)  # Bottleneck layer (compressed representation)
decoded = Dense(10, activation='relu')(encoded)
decoded = Dense(input_dim, activation='sigmoid')(decoded)  # Output layer

# ✅ Compile the Autoencoder
autoencoder = Model(input_layer, decoded)
autoencoder.compile(optimizer='adam', loss='mse')

# ✅ Step 2: Train Autoencoder
autoencoder.fit(X_train, X_train, epochs=50, batch_size=16, shuffle=True, validation_data=(X_test, X_test), verbose=1)

# ✅ Step 3: Extract Encoded Features (For Anomaly Detection)
encoder = Model(input_layer, encoded)  # Extract the encoded representation
X_train_encoded = encoder.predict(X_train)
X_test_encoded = encoder.predict(X_test)

# ✅ Step 4: Train a Classifier on Encoded Features
rf_autoencoder = RandomForestClassifier(n_estimators=100, random_state=42)
rf_autoencoder.fit(X_train_encoded, y_train)

# ✅ Step 5: Evaluate Model
y_pred_auto = rf_autoencoder.predict(X_test_encoded)
accuracy_auto = accuracy_score(y_test, y_pred_auto)
roc_auc_auto = roc_auc_score(y_test, y_pred_auto)

# ✅ Display Performance Metrics
print(f"\n🚀 Autoencoder + Random Forest Performance:")
print(f"✅ Accuracy: {accuracy_auto:.4f}")
print(f"✅ ROC-AUC Score: {roc_auc_auto:.4f}")
print("\nClassification Report:")
print(classification_report(y_test, y_pred_auto))
# ✅ Step 6: Extract Feature Importance from Autoencoder + Random Forest Model

# Get feature importance from the trained Random Forest model on encoded features
feature_importance_auto = rf_autoencoder.feature_importances_

# Create a DataFrame for feature importance
feature_names = ["C Peptide", "HbA1c", "Triglycerides", "Gender", "Race"]  # Adjusted to match X_train_encoded dimensions
df_feature_importance_auto = pd.DataFrame({"Feature": feature_names, "Importance": feature_importance_auto})

# Sort features by importance
df_feature_importance_auto = df_feature_importance_auto.sort_values(by="Importance", ascending=False)

# ✅ Display Feature Importance DataFrame
print("Feature Importance (Autoencoder + Random Forest):")
print(df_feature_importance_auto)

# ✅ Step 7: Plot Feature Importance
import matplotlib.pyplot as plt
import seaborn as sns

plt.figure(figsize=(8, 6))
sns.barplot(x="Importance", y="Feature", data=df_feature_importance_auto, palette="coolwarm")
plt.xlabel("Feature Importance Score")
plt.ylabel("Feature Name")
plt.title("Feature Importance (Autoencoder + Random Forest)")
plt.show()



# In[77]:


# ✅ Step 1: Import Required Libraries for ROC Curve
from sklearn.metrics import roc_curve, auc

# ✅ Step 2: Compute False Positive Rate (FPR), True Positive Rate (TPR), and AUC
fpr_auto, tpr_auto, _ = roc_curve(y_test, y_prob_auto)
roc_auc_auto = auc(fpr_auto, tpr_auto)

# ✅ Step 3: Plot the ROC Curve
plt.figure(figsize=(8, 6))
plt.plot(fpr_auto, tpr_auto, color='blue', lw=2, label=f"AUC = {roc_auc_auto:.4f}")
plt.plot([0, 1], [0, 1], color='grey', linestyle='--')  # Diagonal reference line
plt.xlim([0.0, 1.0])
plt.ylim([0.0, 1.05])
plt.xlabel("False Positive Rate")
plt.ylabel("True Positive Rate")
plt.title("ROC Curve for Autoencoder-Based Model")
plt.legend(loc="lower right")
plt.show()


# In[83]:


# ✅ Step 1: Import Required Libraries
import matplotlib.pyplot as plt
import seaborn as sns

# ✅ Step 2: Count Distribution of Gender
plt.figure(figsize=(6, 5))
sns.countplot(x=df_bio["gender"], palette="coolwarm")
plt.xlabel("Gender")
plt.ylabel("Count")
plt.title("Distribution of Gender in Dataset")
plt.show()

# ✅ Step 3: Gender vs. Diabetes Status
plt.figure(figsize=(6, 5))
sns.countplot(x=df_bio["gender"], hue=df_bio["diabetes_status"], palette="coolwarm")
plt.xlabel("Gender")
plt.ylabel("Count")
plt.title("Diabetes Status by Gender")
plt.legend(title="Diabetes Status", labels=["Non-Diabetic (0)", "Diabetic (1)"])
plt.show()


# In[85]:


# ✅ Step 1: Import Required Libraries
import matplotlib.pyplot as plt
import seaborn as sns

# ✅ Step 2: Set Up a Grid for Side-by-Side Visualization
fig, axes = plt.subplots(2, 3, figsize=(18, 12))

# 1️⃣ Gender Distribution
sns.countplot(x=df_bio["gender"], palette="coolwarm", ax=axes[0, 0])
axes[0, 0].set_title("Gender Distribution")

# 2️⃣ Diabetes Status by Gender
sns.countplot(x=df_bio["gender"], hue=df_bio["diabetes_status"], palette="coolwarm", ax=axes[0, 1])
axes[0, 1].set_title("Diabetes Status by Gender")

# 3️⃣ Race Distribution
sns.countplot(y=df_bio["race"], palette="viridis", ax=axes[0, 2])
axes[0, 2].set_title("Race Distribution")

# 4️⃣ Diabetes Status by Race
sns.countplot(y=df_bio["race"], hue=df_bio["diabetes_status"], palette="viridis", ax=axes[1, 0])
axes[1, 0].set_title("Diabetes Status by Race")

# 5️⃣ HbA1c Levels Distribution
sns.histplot(df_bio["Hemoglobin A1c/Hemoglobin.total in Blood_value_as_number"], bins=30, kde=True, color="blue", ax=axes[1, 1])
axes[1, 1].set_title("HbA1c Levels Distribution")

# 6️⃣ Triglyceride Levels Distribution
sns.histplot(df_bio["Triglyceride [Mass/volume] in Serum or Plasma_value_as_number"], bins=30, kde=True, color="green", ax=axes[1, 2])
axes[1, 2].set_title("Triglyceride Levels Distribution")

# ✅ Adjust Layout and Show Plot
plt.tight_layout()
plt.show()


# In[89]:


df_bio


# In[87]:


# ✅ Step 1: Compute Correlation Matrix Among Biomarkers
correlation_matrix = df_bio[[
    "C peptide [Mass/volume] in Serum or Plasma_value_as_number",
    "Hemoglobin A1c/Hemoglobin.total in Blood_value_as_number",
    "Triglyceride [Mass/volume] in Serum or Plasma_value_as_number",
    "diabetes_status"
]].corr()

# ✅ Step 2: Plot the Heatmap
plt.figure(figsize=(8, 6))
sns.heatmap(correlation_matrix, annot=True, cmap="coolwarm", fmt=".2f", linewidths=0.5)
plt.title("Correlation Matrix Among Biomarkers and Diabetes Status")
plt.show()
# ✅ Step 1: Import Required Libraries
import matplotlib.pyplot as plt
import seaborn as sns

# ✅ Step 2: Boxplots to Compare Biomarkers by Diabetes Status

fig, axes = plt.subplots(1, 3, figsize=(18, 5))

# 1️⃣ Boxplot: HbA1c Levels by Diabetes Status
sns.boxplot(x=df_bio["diabetes_status"], y=df_bio["Hemoglobin A1c/Hemoglobin.total in Blood_value_as_number"], ax=axes[0], palette="coolwarm")
axes[0].set_title("HbA1c Levels by Diabetes Status")
axes[0].set_xlabel("Diabetes Status")
axes[0].set_ylabel("HbA1c (%)")
axes[0].set_xticklabels(["Non-Diabetic", "Diabetic"])

# 2️⃣ Boxplot: C-Peptide Levels by Diabetes Status
sns.boxplot(x=df_bio["diabetes_status"], y=df_bio["C peptide [Mass/volume] in Serum or Plasma_value_as_number"], ax=axes[1], palette="coolwarm")
axes[1].set_title("C-Peptide Levels by Diabetes Status")
axes[1].set_xlabel("Diabetes Status")
axes[1].set_ylabel("C-Peptide (ng/mL)")
axes[1].set_xticklabels(["Non-Diabetic", "Diabetic"])

# 3️⃣ Boxplot: Triglyceride Levels by Diabetes Status
sns.boxplot(x=df_bio["diabetes_status"], y=df_bio["Triglyceride [Mass/volume] in Serum or Plasma_value_as_number"], ax=axes[2], palette="coolwarm")
axes[2].set_title("Triglyceride Levels by Diabetes Status")
axes[2].set_xlabel("Diabetes Status")
axes[2].set_ylabel("Triglycerides (mg/dL)")
axes[2].set_xticklabels(["Non-Diabetic", "Diabetic"])

plt.tight_layout()
plt.show()

# ✅ Step 3: Pie Chart for Gender Distribution
plt.figure(figsize=(6, 6))
df_bio["gender"].value_counts().plot.pie(autopct='%1.1f%%', colors=["blue", "pink"], startangle=90, wedgeprops={'edgecolor': 'black'})
plt.title("Gender Distribution in Dataset")
plt.ylabel("")  # Hide y-axis label
plt.show()

# ✅ Step 4: Pie Chart for Diabetes Status Distribution
plt.figure(figsize=(6, 6))
df_bio["diabetes_status"].value_counts().plot.pie(autopct='%1.1f%%', colors=["lightgreen", "red"], startangle=90, wedgeprops={'edgecolor': 'black'})
plt.title("Diabetes Status Distribution")
plt.ylabel("")  # Hide y-axis label
plt.show()



# In[88]:


# ✅ Get Feature Importance from Autoencoder + Random Forest
import pandas as pd

feature_importance_auto = rf_autoencoder.feature_importances_

# ✅ Store in DataFrame
feature_names = ["C Peptide", "HbA1c", "Triglycerides", "Gender", "Race"]
df_feature_importance_auto = pd.DataFrame({"Feature": feature_names, "Importance": feature_importance_auto})

# ✅ Sort by Importance
df_feature_importance_auto = df_feature_importance_auto.sort_values(by="Importance", ascending=False)

# ✅ Display Weights
print("Feature Importance for Web App:")
print(df_feature_importance_auto)


# In[ ]:


import seaborn as sns
import matplotlib.pyplot as plt

# ✅ Step 1: Ensure required columns exist in the dataset
correlation_columns = [
    "diabetes_risk_score",  # Your ML-based risk score
    "cvd_risk_score",       # Your ML-based CVD risk score
    "metabolic_risk_score", # Your ML-based metabolic risk score
    "Hemoglobin A1c/Hemoglobin.total in Blood_value_as_number", 
    "Triglyceride [Mass/volume] in Serum or Plasma_value_as_number",
    "C peptide [Mass/volume] in Serum or Plasma_value_as_number"
]

# ✅ Step 2: Compute correlation matrix
correlation_matrix = df_bio[correlation_columns].corr()

# ✅ Step 3: Plot the correlation heatmap
plt.figure(figsize=(8, 6))
sns.heatmap(correlation_matrix, annot=True, cmap="coolwarm", fmt=".2f", linewidths=0.5)
plt.title("Correlation Heatmap: ML-Based Risk Scores vs Biomarkers")
plt.show()


# In[ ]:


# ✅ Basic Data Overview
print("Dataset Shape:", df_bio.shape)
print("\nMissing Values:")
print(df_bio.isnull().sum())

# ✅ Data Type Check
print("\nData Types:")
print(df_bio.dtypes)

# ✅ Basic Statistics
print("\nSummary Statistics:")
print(df_bio.describe())



# ✅ Violin Plots for Biomarker Distributions by Diabetes Status
plt.figure(figsize=(12, 5))

plt.subplot(1, 3, 1)
sns.violinplot(x=df_bio["diabetes_status"], y=df_bio["Hemoglobin A1c/Hemoglobin.total in Blood_value_as_number"], palette="coolwarm")
plt.title("HbA1c vs Diabetes Status")

plt.subplot(1, 3, 2)
sns.violinplot(x=df_bio["diabetes_status"], y=df_bio["Triglyceride [Mass/volume] in Serum or Plasma_value_as_number"], palette="coolwarm")
plt.title("Triglycerides vs Diabetes Status")

plt.subplot(1, 3, 3)
sns.violinplot(x=df_bio["diabetes_status"], y=df_bio["C peptide [Mass/volume] in Serum or Plasma_value_as_number"], palette="coolwarm")
plt.title("C-Peptide vs Diabetes Status")

plt.tight_layout()
plt.show()

import seaborn as sns

# ✅ Boxplot for Outlier Detection
plt.figure(figsize=(12, 5))

plt.subplot(1, 3, 1)
sns.boxplot(y=df_bio["Hemoglobin A1c/Hemoglobin.total in Blood_value_as_number"], color='blue')
plt.title("HbA1c Outliers")

plt.subplot(1, 3, 2)
sns.boxplot(y=df_bio["Triglyceride [Mass/volume] in Serum or Plasma_value_as_number"], color='red')
plt.title("Triglyceride Outliers")

plt.subplot(1, 3, 3)
sns.boxplot(y=df_bio["C peptide [Mass/volume] in Serum or Plasma_value_as_number"], color='green')
plt.title("C-Peptide Outliers")

plt.tight_layout()
plt.show()
import matplotlib.pyplot as plt

# ✅ Plot Risk Score Distributions
plt.figure(figsize=(12, 5))

plt.subplot(1, 3, 1)
plt.hist(df_bio["diabetes_risk_score"], bins=20, color='blue', alpha=0.7)
plt.title("Diabetes Risk Distribution")
plt.xlabel("Risk Score (%)")
plt.ylabel("Count")

plt.subplot(1, 3, 2)
plt.hist(df_bio["cvd_risk_score"], bins=20, color='red', alpha=0.7)
plt.title("Cardiovascular Risk Distribution")
plt.xlabel("Risk Score (%)")

plt.subplot(1, 3, 3)
plt.hist(df_bio["metabolic_risk_score"], bins=20, color='green', alpha=0.7)
plt.title("Metabolic Syndrome Risk Distribution")
plt.xlabel("Risk Score (%)")

plt.tight_layout()
plt.show()

