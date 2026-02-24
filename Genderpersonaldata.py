#######################Change History###############
#24/02/2026 #1 Line LIMIT commented out

import os
import pandas as pd
import psycopg2
import gender_guesser.detector as gender

# -------------------------
# Step 1: Redshift connection
# -------------------------
conn = psycopg2.connect(
    host="dw01-prd-laka-co.cxrvmdfr0ysf.eu-west-1.redshift.amazonaws.com",
    port=5439,
    database="dev",
    #enter your username and password
    user="enterusername",
    password="enterpassword"
)

# -------------------------
# Step 2: Fetch all data from DW.people_domain
# -------------------------
query = "SELECT first_name FROM DW.people_domain;"  # no LIMIT
df = pd.read_sql(query, conn)

# Close connection
conn.close()

# -------------------------
# Step 3: Gender enrichment
# -------------------------
detector = gender.Detector()

def gender_prob(name):
    result = detector.get_gender(name)
    if result == "male":
        return 100, 0
    elif result == "female":
        return 0, 100
    elif result == "mostly_male":
        return 75, 25
    elif result == "mostly_female":
        return 25, 75
    elif result == "andy":
        return 50, 50
    else:
        return None, None

df["%_male"], df["%_female"] = zip(*df["first_name"].apply(
    lambda x: gender_prob(x) if pd.notnull(x) else (None, None)
))

# -------------------------
# Step 4: Save CSV safely
# -------------------------
folder = r"G:\My Drive\VSC\Data 921"
os.makedirs(folder, exist_ok=True)  # creates folder if not exists

file_path = os.path.join(folder, "FirstName_.csv") # change the file name
df.to_csv(file_path, index=False)

# -------------------------
# Step 5: Print sample
# -------------------------
print("✅ CSV saved successfully!")
print(f"Saved at: {file_path}")
print("=== Sample of data ===")
#print(df.sample(100)) --#1
print(df) 
print(f"Total rows fetched: {len(df)}")
