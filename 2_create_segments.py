# 2_create_segments.py
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
import matplotlib.pyplot as plt

# --- Database Connection (Same as before) ---
db_user = 'root'
db_password = 'rahul' # IMPORTANT: Update with your password
db_host = 'localhost'
db_name = 'marketing_db'
engine = create_engine(f"mysql+mysqlconnector://{db_user}:{db_password}@{db_host}/{db_name}")

# --- Load RFM Data ---
print("Loading RFM data from MySQL...")
rfm_df = pd.read_sql('customer_rfm', engine, index_col='Customer ID')

# --- Pre-process Data ---
# Handle skewness with log transformation
rfm_log = rfm_df.apply(np.log1p)

# Scale the data
scaler = StandardScaler()
rfm_scaled = scaler.fit_transform(rfm_log)

# --- ADD THIS LINE TO FIX THE ERROR ---
# Replace NaN/infinity with 0
rfm_scaled = np.nan_to_num(rfm_scaled)


# --- Find Optimal Number of Clusters (Elbow Method) ---
print("Finding optimal number of clusters using the Elbow Method...")
sse = {}
for k in range(1, 11):
    kmeans = KMeans(n_clusters=k, random_state=42, n_init='auto') # Set n_init to 'auto' or 10
    kmeans.fit(rfm_scaled)
    sse[k] = kmeans.inertia_

plt.figure()
plt.plot(list(sse.keys()), list(sse.values()), 'o-')
plt.xlabel("Number of clusters")
plt.ylabel("SSE")
plt.title("Elbow Method For Optimal k")
plt.show() # A plot will appear. Look for the "elbow" point. It should be around k=3 or k=4.

# --- Run KMeans Clustering ---
# Based on the elbow plot, let's choose 4 clusters.
optimal_k = 4
print(f"Running KMeans with {optimal_k} clusters...")
kmeans = KMeans(n_clusters=optimal_k, random_state=42, n_init='auto') # Set n_init to 'auto' or 10
kmeans.fit(rfm_scaled)
rfm_df['Cluster'] = kmeans.labels_

# --- Analyze Segments ---
print("Analyzing customer segments...")
segment_analysis = rfm_df.groupby('Cluster').agg({
    'recency': 'mean',
    'frequency': 'mean',
    'monetary': ['mean', 'count']
}).round(2)
print(segment_analysis)

# --- Save Segmented Data to MySQL ---
print("Saving segmented customer data back to MySQL...")
try:
    rfm_df.to_sql('customer_segments', con=engine, if_exists='replace')
    print("Successfully saved data to 'customer_segments' table.")
except Exception as e:
    print(f"Error saving data to MySQL: {e}")