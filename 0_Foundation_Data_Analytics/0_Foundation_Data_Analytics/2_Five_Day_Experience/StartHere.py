# Databricks notebook source
# DBTITLE 1,Overview
# MAGIC %md
# MAGIC # 🎓 EduFinDBX - Production Deployment
# MAGIC
# MAGIC **Enterprise-Grade SQL Investigation Training Platform**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📋 Deployment Overview
# MAGIC
# MAGIC This notebook performs a **complete environment deployment** with automatic configuration, data generation, and validation.
# MAGIC
# MAGIC ### What Gets Deployed
# MAGIC
# MAGIC * **Environment Detection** - Automatic DEV/UAT/PROD configuration from notebook path
# MAGIC * **Unity Catalog Schemas** - Two training schemas with appropriate metadata
# MAGIC * **Synthetic Datasets** - Realistic Indian fintech data with intentional anomalies
# MAGIC   * Small Schema: 3,000 customers, 5,000 loans
# MAGIC   * National Schema: 100,000 customers, 500,000 loans
# MAGIC * **Data Quality Validation** - Automated checks for completeness and integrity
# MAGIC
# MAGIC ### Investigation Structure
# MAGIC
# MAGIC * **Canvas/** - 3 Investigation Cases (Portfolio Health, Customer Risk, Time Analysis)
# MAGIC * **Lead/** - Reference solutions and instructor playbook
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## ▶️ Execution Instructions
# MAGIC
# MAGIC 1. Click **Run All** to begin deployment
# MAGIC 2. Monitor progress through 8 deployment stages
# MAGIC 3. Review deployment summary at completion
# MAGIC 4. Navigate to Canvas/ to start investigations
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### ⏱️ Expected Runtime
# MAGIC
# MAGIC * Small Schema: ~15-20 seconds
# MAGIC * National Schema: ~60-90 seconds
# MAGIC * Total Deployment: ~2-3 minutes
# MAGIC
# MAGIC ### 🔒 Safety Features
# MAGIC
# MAGIC * Automatic environment detection
# MAGIC * Schema isolation per environment
# MAGIC * Reproducible synthetic data (seeded)
# MAGIC * Validation before completion
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Install Dependencies
# ====================================================================
# STAGE 1: INSTALL DEPENDENCIES
# ====================================================================

import time
stage_start = time.time()

print("[1/8] Installing Dependencies...")
print("" + "-" * 60)

%pip install faker --quiet

# Verify installation
try:
    from faker import Faker
    print("\u2713 faker successfully installed")
except ImportError:
    print("\u2717 ERROR: faker installation failed")
    raise

stage_elapsed = time.time() - stage_start
print(f"\u2713 STAGE 1 COMPLETE | {stage_elapsed:.1f}s")
print("=" * 60 + "\n")

# COMMAND ----------

# DBTITLE 1,Environment Detection
# ====================================================================
# STAGE 2: ENVIRONMENT DETECTION & VALIDATION
# ====================================================================

import re
from datetime import datetime, timedelta, date
import time

stage_start = time.time()

print("[2/8] Detecting Environment...")
print("-" * 60)

# Detect environment from notebook path
try:
    current_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
    print(f"Notebook Path: {current_path}")
    
    env_match = re.search(r'/Shared/(dev|uat|prod)/EduFinDBX', current_path, re.IGNORECASE)
    
    if env_match:
        environment = env_match.group(1).lower()
        print(f"\u2713 Environment detected: {environment.upper()}")
    else:
        environment = "dev"
        print("\u26a0️  Warning: Path does not match expected pattern")
        print(f"\u2713 Defaulting to: {environment.upper()}")
    
    # Configure schemas based on environment
    SCHEMA_SMALL = f"workspace.{environment}_edufin_small"
    SCHEMA_NATIONAL = f"workspace.{environment}_edufin_national"
    
    print(f"\u2713 Small Schema: {SCHEMA_SMALL}")
    print(f"\u2713 National Schema: {SCHEMA_NATIONAL}")
    
    # Verify Unity Catalog access
    try:
        spark.sql("SHOW CATALOGS LIKE 'workspace'").collect()
        print("\u2713 Unity Catalog access confirmed")
    except Exception as uc_error:
        print(f"\u2717 ERROR: Cannot access Unity Catalog")
        print(f"   Details: {uc_error}")
        raise
    
except Exception as e:
    print(f"\u2717 CRITICAL ERROR: Environment detection failed")
    print(f"   Details: {e}")
    print("\n   Required: Notebook must be in /Shared/<env>/EduFinDBX/")
    print("   Valid environments: dev, uat, prod")
    dbutils.notebook.exit('{"status": "FAIL", "message": "Environment detection failed"}')

stage_elapsed = time.time() - stage_start
print(f"\u2713 STAGE 2 COMPLETE | {stage_elapsed:.1f}s")
print("=" * 60 + "\n")

# COMMAND ----------

# DBTITLE 1,Create Schemas
# ====================================================================
# STAGE 3: CREATE UNITY CATALOG SCHEMAS
# ====================================================================

stage_start = time.time()

print("[3/8] Creating Schemas...")
print("-" * 60)

try:
    # Create small schema
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_SMALL}")
    spark.sql(f"COMMENT ON SCHEMA {SCHEMA_SMALL} IS 'EduFinDBX Small Training Dataset ({environment.upper()}) - 3K customers, 5K loans for rapid iteration'")
    print(f"\u2713 Created: {SCHEMA_SMALL}")
    
    # Create national schema
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_NATIONAL}")
    spark.sql(f"COMMENT ON SCHEMA {SCHEMA_NATIONAL} IS 'EduFinDBX National Training Dataset ({environment.upper()}) - 100K customers, 500K loans for production-scale queries'")
    print(f"\u2713 Created: {SCHEMA_NATIONAL}")
    
    # Verify schema creation
    small_exists = spark.sql(f"SHOW SCHEMAS LIKE '{SCHEMA_SMALL.split('.')[1]}'").count() > 0
    national_exists = spark.sql(f"SHOW SCHEMAS LIKE '{SCHEMA_NATIONAL.split('.')[1]}'").count() > 0
    
    if small_exists and national_exists:
        print("\u2713 Schema validation: Both schemas confirmed")
    else:
        raise Exception("Schema creation validation failed")
        
except Exception as e:
    print(f"\u2717 ERROR: Schema creation failed")
    print(f"   Details: {e}")
    raise

stage_elapsed = time.time() - stage_start
print(f"\u2713 STAGE 3 COMPLETE | {stage_elapsed:.1f}s")
print("=" * 60 + "\n")

# COMMAND ----------

# DBTITLE 1,Drop Existing Tables
# ====================================================================
# STAGE 4: PRE-GENERATION CLEANUP (OPTIONAL)
# ====================================================================

stage_start = time.time()

print("[4/8] Pre-Generation Cleanup...")
print("-" * 60)
print(f"\u26a0️  Environment: {environment.upper()}")
print("\u26a0️  Action: Dropping existing tables for clean regeneration")
print("-" * 60)

tables_to_drop = ["dim_state", "dim_city", "institutions", "customers", "loans"]
tables_dropped = 0

for schema in [SCHEMA_SMALL, SCHEMA_NATIONAL]:
    for table in tables_to_drop:
        try:
            spark.sql(f"DROP TABLE IF EXISTS {schema}.{table}")
            tables_dropped += 1
        except Exception as e:
            print(f"\u26a0️  Could not drop {schema}.{table}: {e}")

print(f"\u2713 Cleanup complete: {tables_dropped} table(s) processed")
print(f"\u2713 Ready for fresh data generation")

stage_elapsed = time.time() - stage_start
print(f"\u2713 STAGE 4 COMPLETE | {stage_elapsed:.1f}s")
print("=" * 60 + "\n")

# COMMAND ----------

# DBTITLE 1,Reference Data
# ====================================================================
# STAGE 5: LOAD REFERENCE DATA
# ====================================================================

# Reference data: States, Cities, Institutions
# Includes intentional anomalies for investigation cases

STATES = [
    {"state_id": 1, "state_name": "Andhra Pradesh", "region": "South"},
    {"state_id": 2, "state_name": "Karnataka", "region": "South"},
    {"state_id": 3, "state_name": "Maharashtra", "region": "West"},
    {"state_id": 4, "state_name": "Telangana", "region": "South"},
    {"state_id": 5, "state_name": "Tamil Nadu", "region": "South"},
    {"state_id": 6, "state_name": "Assam", "region": "East"},
    {"state_id": 7, "state_name": "Bihar", "region": "East"},
    {"state_id": 8, "state_name": "Madhya Pradesh", "region": "Central"},
    {"state_id": 9, "state_name": "Rajasthan", "region": "West"},
    {"state_id": 10, "state_name": "Gujarat", "region": "West"},
    {"state_id": 11, "state_name": "Kerala", "region": "South"},
    {"state_id": 12, "state_name": "Uttar Pradesh", "region": "North"},
    {"state_id": 13, "state_name": "West Bengal", "region": "East"},
    {"state_id": 14, "state_name": "Punjab", "region": "North"},
    {"state_id": 15, "state_name": "Chhattisgarh", "region": "Central"},
    {"state_id": 16, "state_name": "Jharkhand", "region": "East"},
    {"state_id": 17, "state_name": "Odisha", "region": "East"},
    {"state_id": 18, "state_name": "Delhi", "region": "North"},
]

CITIES = [
    {"city_id": 1, "city_name": "Bengaluru", "state_id": 2, "tier_classification": "Tier1"},
    {"city_id": 2, "city_name": "Mumbai", "state_id": 3, "tier_classification": "Tier1"},
    {"city_id": 3, "city_name": "Hyderabad", "state_id": 4, "tier_classification": "Tier1"},
    {"city_id": 4, "city_name": "Chennai", "state_id": 5, "tier_classification": "Tier1"},
    {"city_id": 5, "city_name": "Pune", "state_id": 3, "tier_classification": "Tier1"},
    {"city_id": 6, "city_name": "Ahmedabad", "state_id": 10, "tier_classification": "Tier1"},
    {"city_id": 7, "city_name": "Kolkata", "state_id": 13, "tier_classification": "Tier1"},
    {"city_id": 8, "city_name": "Delhi", "state_id": 18, "tier_classification": "Tier1"},
    {"city_id": 9, "city_name": "Jaipur", "state_id": 9, "tier_classification": "Tier1"},
    {"city_id": 10, "city_name": "Lucknow", "state_id": 12, "tier_classification": "Tier1"},
    {"city_id": 11, "city_name": "Kochi", "state_id": 11, "tier_classification": "Tier1"},
    {"city_id": 12, "city_name": "Indore", "state_id": 8, "tier_classification": "Tier1"},
    {"city_id": 13, "city_name": "Surat", "state_id": 10, "tier_classification": "Tier1"},
    {"city_id": 14, "city_name": "Nagpur", "state_id": 3, "tier_classification": "Tier1"},
    {"city_id": 15, "city_name": "Thane", "state_id": 3, "tier_classification": "Tier1"},
    {"city_id": 16, "city_name": "Coimbatore", "state_id": 5, "tier_classification": "Tier1"},
    {"city_id": 17, "city_name": "Noida", "state_id": 12, "tier_classification": "Tier1"},
    {"city_id": 18, "city_name": "Gurgaon", "state_id": 18, "tier_classification": "Tier1"},
    {"city_id": 19, "city_name": "Visakhapatnam", "state_id": 1, "tier_classification": "Tier2"},
    {"city_id": 20, "city_name": "Vijayawada", "state_id": 1, "tier_classification": "Tier2"},
    {"city_id": 21, "city_name": "Tirupati", "state_id": 1, "tier_classification": "Tier2"},
    {"city_id": 22, "city_name": "Guwahati", "state_id": 6, "tier_classification": "Tier2"},
    {"city_id": 23, "city_name": "Patna", "state_id": 7, "tier_classification": "Tier2"},
    {"city_id": 24, "city_name": "Bhopal", "state_id": 8, "tier_classification": "Tier2"},
    {"city_id": 25, "city_name": "Gwalior", "state_id": 8, "tier_classification": "Tier2"},
    {"city_id": 26, "city_name": "Ranchi", "state_id": 16, "tier_classification": "Tier2"},
    {"city_id": 27, "city_name": "Jamshedpur", "state_id": 16, "tier_classification": "Tier2"},
    {"city_id": 28, "city_name": "Raipur", "state_id": 15, "tier_classification": "Tier2"},
    {"city_id": 29, "city_name": "Mysuru", "state_id": 2, "tier_classification": "Tier2"},
    {"city_id": 30, "city_name": "Mangaluru", "state_id": 2, "tier_classification": "Tier2"},
    {"city_id": 31, "city_name": "Thiruvananthapuram", "state_id": 11, "tier_classification": "Tier2"},
    {"city_id": 32, "city_name": "Amritsar", "state_id": 14, "tier_classification": "Tier2"},
    {"city_id": 33, "city_name": "Varanasi", "state_id": 12, "tier_classification": "Tier2"},
    {"city_id": 34, "city_name": "Agra", "state_id": 12, "tier_classification": "Tier2"},
    {"city_id": 35, "city_name": "Nashik", "state_id": 3, "tier_classification": "Tier2"},
    {"city_id": 36, "city_name": "Madurai", "state_id": 5, "tier_classification": "Tier2"},
    {"city_id": 37, "city_name": "Udaipur", "state_id": 9, "tier_classification": "Tier2"},
    {"city_id": 38, "city_name": "Bhubaneswar", "state_id": 17, "tier_classification": "Tier3"},
    {"city_id": 39, "city_name": "Dehradun", "state_id": 12, "tier_classification": "Tier3"},
    {"city_id": 40, "city_name": "Siliguri", "state_id": 13, "tier_classification": "Tier3"},
    {"city_id": 41, "city_name": "Jabalpur", "state_id": 8, "tier_classification": "Tier3"},
    {"city_id": 42, "city_name": "Howrah", "state_id": 13, "tier_classification": "Tier3"},
    {"city_id": 43, "city_name": "Salem", "state_id": 5, "tier_classification": "Tier3"},
    {"city_id": 44, "city_name": "Warangal", "state_id": 4, "tier_classification": "Tier3"},
    {"city_id": 45, "city_name": "Kota", "state_id": 9, "tier_classification": "Tier3"},
]

INSTITUTIONS = [
    {'institution_id': 1, 'institution_name': 'National Institute of Technology', 'institution_type': 'Engineering', 'accreditation_grade': 'A+', 'placement_rate': 92.0, 'established_year': 1965},
    {'institution_id': 2, 'institution_name': 'Indian Institute of Engineering Science', 'institution_type': 'Engineering', 'accreditation_grade': 'A+', 'placement_rate': 88.0, 'established_year': 1978},
    {'institution_id': 3, 'institution_name': 'Rajiv Gandhi Technical University', 'institution_type': 'Engineering', 'accreditation_grade': 'A', 'placement_rate': 74.0, 'established_year': 1990},
    {'institution_id': 4, 'institution_name': 'Birla Institute of Applied Sciences', 'institution_type': 'Engineering', 'accreditation_grade': 'A', 'placement_rate': 79.0, 'established_year': 1982},
    {'institution_id': 5, 'institution_name': 'Guru Gobind Singh Engineering College', 'institution_type': 'Engineering', 'accreditation_grade': 'B', 'placement_rate': 62.0, 'established_year': 2001},
    {'institution_id': 6, 'institution_name': 'Maulana Azad National Institute of Tech', 'institution_type': 'Engineering', 'accreditation_grade': 'A+', 'placement_rate': 85.0, 'established_year': 1960},
    {'institution_id': 7, 'institution_name': 'Sathyabama Institute of Science', 'institution_type': 'Engineering', 'accreditation_grade': 'B', 'placement_rate': 58.0, 'established_year': 1988},
    {'institution_id': 8, 'institution_name': 'Visvesvaraya Technological University', 'institution_type': 'Engineering', 'accreditation_grade': 'A', 'placement_rate': 71.0, 'established_year': 1998},
    {'institution_id': 9, 'institution_name': 'Shri Ramdeobaba College of Engineering', 'institution_type': 'Engineering', 'accreditation_grade': 'B', 'placement_rate': 65.0, 'established_year': 1984},
    {'institution_id': 10, 'institution_name': 'Government Engineering College', 'institution_type': 'Engineering', 'accreditation_grade': 'C', 'placement_rate': 42.0, 'established_year': 2005},
    {'institution_id': 11, 'institution_name': 'All India Institute of Medical Sciences', 'institution_type': 'Medical', 'accreditation_grade': 'A+', 'placement_rate': 95.0, 'established_year': 1956},
    {'institution_id': 12, 'institution_name': 'Christian Medical College', 'institution_type': 'Medical', 'accreditation_grade': 'A+', 'placement_rate': 93.0, 'established_year': 1900},
    {'institution_id': 13, 'institution_name': 'Kasturba Medical College', 'institution_type': 'Medical', 'accreditation_grade': 'A', 'placement_rate': 87.0, 'established_year': 1953},
    {'institution_id': 14, 'institution_name': 'Sri Ramachandra Medical College', 'institution_type': 'Medical', 'accreditation_grade': 'A', 'placement_rate': 82.0, 'established_year': 1985},
    {'institution_id': 15, 'institution_name': 'Government Medical College', 'institution_type': 'Medical', 'accreditation_grade': 'B', 'placement_rate': 68.0, 'established_year': 1970},
    {'institution_id': 16, 'institution_name': 'Regional Institute of Medical Sciences', 'institution_type': 'Medical', 'accreditation_grade': 'B', 'placement_rate': 61.0, 'established_year': 1993},
    {'institution_id': 17, 'institution_name': 'Indian School of Business', 'institution_type': 'Management', 'accreditation_grade': 'A+', 'placement_rate': 94.0, 'established_year': 2001},
    {'institution_id': 18, 'institution_name': 'Symbiosis Institute of Management', 'institution_type': 'Management', 'accreditation_grade': 'A', 'placement_rate': 80.0, 'established_year': 1978},
    {'institution_id': 19, 'institution_name': 'ICFAI Business School', 'institution_type': 'Management', 'accreditation_grade': 'A', 'placement_rate': 76.0, 'established_year': 1995},
    {'institution_id': 20, 'institution_name': 'Narsee Monjee Institute of Management', 'institution_type': 'Management', 'accreditation_grade': 'A', 'placement_rate': 78.0, 'established_year': 1981},
    {'institution_id': 21, 'institution_name': 'City Business Academy', 'institution_type': 'Management', 'accreditation_grade': 'B', 'placement_rate': 55.0, 'established_year': 2008},
    {'institution_id': 22, 'institution_name': 'Regional Management Institute', 'institution_type': 'Management', 'accreditation_grade': 'C', 'placement_rate': 44.0, 'established_year': 2012},
    {'institution_id': 23, 'institution_name': 'Jawaharlal Nehru University', 'institution_type': 'Arts', 'accreditation_grade': 'A+', 'placement_rate': 60.0, 'established_year': 1969},
    {'institution_id': 24, 'institution_name': 'Delhi University Arts Faculty', 'institution_type': 'Arts', 'accreditation_grade': 'A', 'placement_rate': 52.0, 'established_year': 1922},
    {'institution_id': 25, 'institution_name': 'Presidency University', 'institution_type': 'Arts', 'accreditation_grade': 'A', 'placement_rate': 48.0, 'established_year': 1817},
    {'institution_id': 26, 'institution_name': 'Loyola College', 'institution_type': 'Arts', 'accreditation_grade': 'A', 'placement_rate': 56.0, 'established_year': 1925},
    {'institution_id': 27, 'institution_name': 'State Arts and Science College', 'institution_type': 'Arts', 'accreditation_grade': 'B', 'placement_rate': 40.0, 'established_year': 1985},
    {'institution_id': 28, 'institution_name': 'Municipal Arts College', 'institution_type': 'Arts', 'accreditation_grade': 'C', 'placement_rate': 35.0, 'established_year': 2000},
    {'institution_id': 29, 'institution_name': 'Global Vocational Training Centre', 'institution_type': 'Vocational', 'accreditation_grade': 'B', 'placement_rate': 50.0, 'established_year': 2010},
    {'institution_id': 30, 'institution_name': 'National Skill Development Institute', 'institution_type': 'Vocational', 'accreditation_grade': 'B', 'placement_rate': 47.0, 'established_year': 2015},
    {'institution_id': 31, 'institution_name': 'Standard Polytechnic', 'institution_type': 'Polytechnic', 'accreditation_grade': 'B', 'placement_rate': 52.0, 'established_year': 1995},
    {'institution_id': 32, 'institution_name': 'Central Polytechnic Institute', 'institution_type': 'Polytechnic', 'accreditation_grade': 'C', 'placement_rate': 38.0, 'established_year': 2003},
    {'institution_id': 33, 'institution_name': 'Rural Polytechnic College', 'institution_type': 'Polytechnic', 'accreditation_grade': 'C', 'placement_rate': 32.0, 'established_year': 2009},
    {'institution_id': 34, 'institution_name': 'District Vocational Centre', 'institution_type': 'Vocational', 'accreditation_grade': 'C', 'placement_rate': 28.0, 'established_year': 2018},
    {'institution_id': 35, 'institution_name': 'Shri Ram College of Commerce', 'institution_type': 'Commerce', 'accreditation_grade': 'A+', 'placement_rate': 82.0, 'established_year': 1926},
    {'institution_id': 36, 'institution_name': 'City Commerce College', 'institution_type': 'Commerce', 'accreditation_grade': 'B', 'placement_rate': 45.0, 'established_year': 1990},
    {'institution_id': 37, 'institution_name': 'National Law School of India', 'institution_type': 'Law', 'accreditation_grade': 'A+', 'placement_rate': 90.0, 'established_year': 1987},
    {'institution_id': 38, 'institution_name': 'Central Law Institute', 'institution_type': 'Law', 'accreditation_grade': 'A', 'placement_rate': 72.0, 'established_year': 1970},
    {'institution_id': 39, 'institution_name': 'Regional Law College', 'institution_type': 'Law', 'accreditation_grade': 'B', 'placement_rate': 48.0, 'established_year': 2005},
    {'institution_id': 40, 'institution_name': 'Andhra University College of Engineering', 'institution_type': 'Engineering', 'accreditation_grade': 'A', 'placement_rate': 70.0, 'established_year': 1933},
    {'institution_id': 41, 'institution_name': 'GITAM University', 'institution_type': 'Engineering', 'accreditation_grade': 'A', 'placement_rate': 73.0, 'established_year': 1980},
    {'institution_id': 42, 'institution_name': 'Osmania University', 'institution_type': 'Engineering', 'accreditation_grade': 'B', 'placement_rate': 58.0, 'established_year': 1918},
    {'institution_id': 43, 'institution_name': 'Anna University', 'institution_type': 'Engineering', 'accreditation_grade': 'A+', 'placement_rate': 83.0, 'established_year': 1978},
    {'institution_id': 44, 'institution_name': 'Savitribai Phule Pune University', 'institution_type': 'Engineering', 'accreditation_grade': 'A', 'placement_rate': 69.0, 'established_year': 1949},
    {'institution_id': 45, 'institution_name': 'Jadavpur University', 'institution_type': 'Engineering', 'accreditation_grade': 'A+', 'placement_rate': 86.0, 'established_year': 1955},
    {'institution_id': 46, 'institution_name': 'Amity University', 'institution_type': 'Management', 'accreditation_grade': 'B', 'placement_rate': 54.0, 'established_year': 2005},
    {'institution_id': 47, 'institution_name': 'Manipal Academy of Higher Education', 'institution_type': 'Medical', 'accreditation_grade': 'A+', 'placement_rate': 89.0, 'established_year': 1953},
    {'institution_id': 48, 'institution_name': 'SRM Institute of Science and Technology', 'institution_type': 'Engineering', 'accreditation_grade': 'A', 'placement_rate': 67.0, 'established_year': 1985},
    {'institution_id': 49, 'institution_name': 'Lovely Professional University', 'institution_type': 'Engineering', 'accreditation_grade': 'B', 'placement_rate': 51.0, 'established_year': 2005},
    {'institution_id': 50, 'institution_name': 'Unverified Training Academy', 'institution_type': 'Vocational', 'accreditation_grade': 'C', 'placement_rate': 150.0, 'established_year': 2020}
]

stage_start = time.time()

print("[5/8] Loading Reference Data...")
print("-" * 60)

print(f"\u2713 States loaded: {len(STATES)} records")
print(f"\u2713 Cities loaded: {len(CITIES)} records")
print(f"   - Tier1: {len([c for c in CITIES if c['tier_classification'] == 'Tier1'])} cities")
print(f"   - Tier2: {len([c for c in CITIES if c['tier_classification'] == 'Tier2'])} cities")
print(f"   - Tier3: {len([c for c in CITIES if c['tier_classification'] == 'Tier3'])} cities")
print(f"\u2713 Institutions loaded: {len(INSTITUTIONS)} records")
print(f"   - Engineering: {len([i for i in INSTITUTIONS if i['institution_type'] == 'Engineering'])}")
print(f"   - Medical: {len([i for i in INSTITUTIONS if i['institution_type'] == 'Medical'])}")
print(f"   - Management: {len([i for i in INSTITUTIONS if i['institution_type'] == 'Management'])}")
print(f"   - Other types: {len([i for i in INSTITUTIONS if i['institution_type'] not in ['Engineering', 'Medical', 'Management']])}")
print("\u2713 Intentional anomalies embedded for investigation")

stage_elapsed = time.time() - stage_start
print(f"\u2713 STAGE 5 COMPLETE | {stage_elapsed:.1f}s")
print("=" * 60 + "\n")

# COMMAND ----------

# DBTITLE 1,Generator Configuration
# ====================================================================
# STAGE 6: CONFIGURE DATA GENERATORS
# ====================================================================

from faker import Faker
import random
import numpy as np
from pyspark.sql import Row
from pyspark.sql import functions as F

stage_start = time.time()

print("[6/8] Configuring Data Generators...")
print("-" * 60)

# Initialize generators with fixed seeds for reproducibility
Faker.seed(42)
random.seed(42)
np.random.seed(42)
fake = Faker('en_IN')
print("\u2713 Faker initialized (locale: en_IN, seed: 42)")
print("\u2713 Random generators seeded for reproducibility")

# Dual schema configuration
SCHEMAS = [
    {"schema_name": SCHEMA_SMALL, "NUM_CUSTOMERS": 3000, "NUM_LOANS": 5000},
    {"schema_name": SCHEMA_NATIONAL, "NUM_CUSTOMERS": 100000, "NUM_LOANS": 500000},
]

VIZAG_CITY_ID = 19  # Used for intentional Vizag default anomaly

print(f"\u2713 Schema configurations:")
for config in SCHEMAS:
    print(f"   - {config['schema_name']}: {config['NUM_CUSTOMERS']:,} customers, {config['NUM_LOANS']:,} loans")

stage_elapsed = time.time() - stage_start
print(f"\u2713 STAGE 6 COMPLETE | {stage_elapsed:.1f}s")
print("=" * 60 + "\n")

# COMMAND ----------

# DBTITLE 1,Generate Tables
# ====================================================================
# STAGE 7: GENERATE SYNTHETIC DATASETS
# ====================================================================

overall_start = time.time()

print("[7/8] Generating Synthetic Datasets...")
print("-" * 60)

# Main data generation loop for both schemas
for idx, config in enumerate(SCHEMAS, 1):
    schema = config['schema_name']
    NUM_CUSTOMERS = config['NUM_CUSTOMERS']
    NUM_LOANS = config['NUM_LOANS']
    
    print(f"\nDataset {idx}/2: {schema}")
    print(f"Target: {NUM_CUSTOMERS:,} customers, {NUM_LOANS:,} loans")
    print("-" * 40)
    t0 = time.time()

    # Create dimension tables
    df_states = spark.createDataFrame([Row(**s) for s in STATES])
    df_cities = spark.createDataFrame([Row(**c) for c in CITIES])
    
    # Add tier column to institutions and fix anomalies
    institutions_with_tier = []
    for inst in INSTITUTIONS:
        inst_copy = inst.copy()
        inst_copy["tier"] = "Corporate Partner" if inst["institution_id"] == 17 else None
        institutions_with_tier.append(inst_copy)
    
    df_inst = spark.createDataFrame([Row(**i) for i in institutions_with_tier])
    # Fix anomaly: Institution 50 has impossible 150% placement rate
    df_inst = df_inst.withColumn(
        "placement_rate",
        F.when(F.col("institution_id") == 50, 150.0).otherwise(F.col("placement_rate"))
    )
    print("  \u2713 Dimension tables prepared")
    
    # Write dimension tables
    print("  \u23f3 Writing dimension tables...")
    df_states.write.format("delta").mode("overwrite").saveAsTable(f"{schema}.dim_state")
    df_cities.write.format("delta").mode("overwrite").saveAsTable(f"{schema}.dim_city")
    df_inst.write.format("delta").mode("overwrite").saveAsTable(f"{schema}.institutions")
    print("  \u2713 dim_state, dim_city, institutions created")

    # Generate customers
    print(f"  \u23f3 Generating {NUM_CUSTOMERS:,} customers...")
    tier1_ids = [c["city_id"] for c in CITIES if c["tier_classification"] == "Tier1"]
    tier2_ids = [c["city_id"] for c in CITIES if c["tier_classification"] == "Tier2"]
    tier3_ids = [c["city_id"] for c in CITIES if c["tier_classification"] == "Tier3"]
    
    customers_data = []
    for i in range(1, NUM_CUSTOMERS + 1):
        if i <= NUM_CUSTOMERS * 0.15:
            city_id = random.choice(tier1_ids)
        elif i <= NUM_CUSTOMERS * 0.6:
            city_id = random.choice(tier2_ids)
        else:
            city_id = random.choice(tier3_ids)
        
        customers_data.append({
            "customer_id": i,
            "customer_name": fake.name(),
            "city_id": city_id,
            "phone_number": fake.phone_number(),
            "email": fake.email(),
            "gender": random.choice(["Male", "Female", "Other"]),
            "age": random.randint(18, 45),
            "family_income": random.randint(15000, 120000),
            "credit_score": random.randint(300, 850),
            "registration_date": fake.date_between(start_date='-3y', end_date='today')
        })
    
    # Add edge cases for investigation
    if len(customers_data) >= 100:
        customers_data[50]["credit_score"] = 250
        customers_data[75]["age"] = 17
        customers_data[99]["email"] = "invalid_email"
    
    df_customers = spark.createDataFrame([Row(**c) for c in customers_data])
    df_customers.write.format("delta").mode("overwrite").saveAsTable(f"{schema}.customers")
    print(f"  \u2713 customers table created ({len(customers_data):,} rows)")

    # Generate loans
    print(f"  \u23f3 Generating {NUM_LOANS:,} loans...")
    loans_data = []
    for i in range(1, NUM_LOANS + 1):
        customer_id = random.randint(1, NUM_CUSTOMERS)
        institution_id = random.randint(1, 50)
        loan_amount = random.randint(50000, 1500000)
        interest_rate = round(random.uniform(6.5, 14.5), 2)
        
        disbursement_date = fake.date_between(start_date='-3y', end_date='today')
        tenure_months = random.choice([12, 24, 36, 48, 60, 72, 84, 96, 120])
        
        repayment_status = random.choices(
            ["On-time", "Delayed", "Defaulted"],
            weights=[0.70, 0.20, 0.10]
        )[0]
        
        overdue_days = 0
        if repayment_status == "Delayed":
            overdue_days = random.randint(1, 90)
        elif repayment_status == "Defaulted":
            overdue_days = random.randint(91, 365)
        
        loans_data.append({
            "loan_id": i,
            "customer_id": customer_id,
            "institution_id": institution_id,
            "loan_amount": loan_amount,
            "interest_rate": interest_rate,
            "disbursement_date": disbursement_date,
            "tenure_months": tenure_months,
            "repayment_status": repayment_status,
            "overdue_days": overdue_days
        })
    
    # Add Vizag anomaly for investigation
    vizag_customer_ids = [row.customer_id for row in df_customers.filter(F.col("city_id") == VIZAG_CITY_ID).select("customer_id").collect()]
    if vizag_customer_ids and len(vizag_customer_ids) >= 10:
        for idx, cust_id in enumerate(vizag_customer_ids[:10]):
            if idx < len(loans_data):
                loans_data[idx]["customer_id"] = cust_id
                loans_data[idx]["repayment_status"] = "Defaulted"
                loans_data[idx]["overdue_days"] = random.randint(180, 365)
    
    # Add edge cases
    if len(loans_data) >= 100:
        loans_data[25]["interest_rate"] = 0.0
        loans_data[50]["loan_amount"] = -50000
        loans_data[75]["tenure_months"] = 0
    
    df_loans = spark.createDataFrame([Row(**l) for l in loans_data])
    df_loans.write.format("delta").mode("overwrite").saveAsTable(f"{schema}.loans")
    print(f"  \u2713 loans table created ({len(loans_data):,} rows)")
    
    elapsed = int(time.time() - t0)
    print(f"  \u2713 {schema} complete in {elapsed}s")
    print("-" * 40)

overall_elapsed = time.time() - overall_start
print(f"\n\u2713 All tables generated successfully")
print(f"\u2713 Total tables created: {len(SCHEMAS) * 5}")
print(f"\u2713 STAGE 7 COMPLETE | {overall_elapsed:.1f}s")
print("=" * 60 + "\n")

# COMMAND ----------

# DBTITLE 1,Validate Tables
# ====================================================================
# STAGE 8: VALIDATION & QUALITY CHECKS
# ====================================================================

stage_start = time.time()

print("[8/8] Validating Deployment...")
print("-" * 60)

# Validate table readiness with detailed error reporting
def check_table(schema, table):
    """Check table existence and return row count with detailed error handling."""
    try:
        count = spark.sql(f"SELECT COUNT(*) as cnt FROM {schema}.{table}").collect()[0]['cnt']
        return True, count, None
    except Exception as e:
        error_msg = str(e)
        if "Table or view not found" in error_msg:
            return False, 0, "Table does not exist"
        elif "Permission denied" in error_msg:
            return False, 0, "Permission denied"
        else:
            return False, 0, f"Query failed: {error_msg[:100]}"

tables_to_validate = [
    (SCHEMA_SMALL, "dim_state", 18),
    (SCHEMA_SMALL, "dim_city", 45),
    (SCHEMA_SMALL, "institutions", 50),
    (SCHEMA_SMALL, "customers", 3000),
    (SCHEMA_SMALL, "loans", 5000),
    (SCHEMA_NATIONAL, "dim_state", 18),
    (SCHEMA_NATIONAL, "dim_city", 45),
    (SCHEMA_NATIONAL, "institutions", 50),
    (SCHEMA_NATIONAL, "customers", 100000),
    (SCHEMA_NATIONAL, "loans", 500000)
]

print("Validating table availability and row counts...\n")

all_valid = True
validation_results = []

for schema, table, expected in tables_to_validate:
    has_data, count, error = check_table(schema, table)
    
    if has_data:
        status = "\u2713"
        count_match = count >= expected  # Allow >= for flexibility
        if not count_match:
            status = "\u26a0️ "
            all_valid = False
        print(f"{status} {schema}.{table}: {count:,} rows (expected: {expected:,})")
        validation_results.append((table, "PASS" if count_match else "WARN", count))
    else:
        print(f"\u2717 {schema}.{table}: {error}")
        all_valid = False
        validation_results.append((table, "FAIL", 0))

print("\n" + "-" * 60)
if all_valid:
    print("\u2713 All validations passed")
else:
    print("\u26a0️  Some validations failed - review errors above")

stage_elapsed = time.time() - stage_start
print(f"\u2713 STAGE 8 COMPLETE | {stage_elapsed:.1f}s")
print("=" * 60 + "\n")

# COMMAND ----------

# DBTITLE 1,Deployment Summary
# ====================================================================
# DEPLOYMENT SUMMARY & REPORT
# ====================================================================

from datetime import datetime

print("\n" + "=" * 60)
print("  EDUFINDBX DEPLOYMENT SUMMARY")
print("=" * 60)

if all_valid:
    print("\n\u2705 DEPLOYMENT STATUS: SUCCESS")
    print("-" * 60)
    
    # Environment Details
    print("\n🌐 ENVIRONMENT CONFIGURATION")
    print(f"  Environment: {environment.upper()}")
    print(f"  Catalog: workspace")
    print(f"  Deployment Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Schema Details
    print("\n[SCHEMAS DEPLOYED]")
    print(f"  Small Training Schema:    {SCHEMA_SMALL}")
    print(f"  National Training Schema: {SCHEMA_NATIONAL}")
    
    # Table Summary
    print("\n[TABLES GENERATED]")
    small_total = sum([count for table, status, count in validation_results[:5]])
    national_total = sum([count for table, status, count in validation_results[5:]])
    
    print(f"  {SCHEMA_SMALL}:")
    print(f"    - dim_state: 18 rows")
    print(f"    - dim_city: 45 rows")
    print(f"    - institutions: 50 rows")
    print(f"    - customers: 3,000 rows")
    print(f"    - loans: 5,000 rows")
    print(f"    Total: {small_total:,} rows")
    
    print(f"\n  {SCHEMA_NATIONAL}:")
    print(f"    - dim_state: 18 rows")
    print(f"    - dim_city: 45 rows")
    print(f"    - institutions: 50 rows")
    print(f"    - customers: 100,000 rows")
    print(f"    - loans: 500,000 rows")
    print(f"    Total: {national_total:,} rows")
    
    # Quality Checks
    print("\n\u2705 QUALITY VALIDATIONS")
    print("  \u2713 All tables created successfully")
    print("  \u2713 Row counts validated")
    print("  \u2713 Intentional anomalies embedded")
    print("  \u2713 Reproducible synthetic data (seed: 42)")
    
    # Next Steps
    print("\n[NEXT STEPS]")
    print("  1. Navigate to /Shared/dev/EduFinDBX/Canvas/")
    print("  2. Start with: 1_Portfolio_Health investigation")
    print("  3. Continue with: 2_Customer_Risk investigation")
    print("  4. Complete with: 3_Time_Analysis investigation")
    
    # Advanced Features
    print("\n[AFTER COMPLETING CASES]")
    print("  - Build Genie Space for natural language Q&A")
    print("  - Create Lakeview Dashboard for visualization")
    print("  - Setup Workflow automation for recurring tasks")
    print("  - Review INSTRUCTOR_PLAYBOOK.md in Lead/ folder")
    
    print("\n" + "=" * 60)
    print("  \u2705 DEPLOYMENT COMPLETE - READY FOR TRAINING")
    print("=" * 60 + "\n")
    
else:
    print("\n\u274c DEPLOYMENT STATUS: INCOMPLETE")
    print("-" * 60)
    print("\u26a0️  Some tables failed validation")
    print("\u26a0️  Review error messages in Stage 8 above")
    print("\nTroubleshooting:")
    print("  1. Check Unity Catalog permissions")
    print("  2. Verify workspace catalog access")
    print("  3. Review generation logs for errors")
    print("  4. Re-run failed stages if needed")
    print("\n" + "=" * 60 + "\n")