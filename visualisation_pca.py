import matplotlib.pyplot as plt
from sklearn.decomposition import PCA
from sklearn.cluster import KMeans
import hdbscan

from load_client_data import stream_user_metrics_by_id_range
from load_client_data import build_preprocessor_from_sample

# Charger les données
preprocessor = build_preprocessor_from_sample(sample_size=100000)
X = stream_user_metrics_by_id_range(preprocessor, start_id=0, end_id=1000)

# Réduction de dimension pour visualisation
pca = PCA(n_components=2)
X_pca = pca.fit_transform(X)

# KMeans clustering
k = 3
kmeans = KMeans(n_clusters=k, random_state=42)
kmeans_labels = kmeans.fit_predict(X)

# HDBSCAN clustering
hdb = hdbscan.HDBSCAN(min_cluster_size=10)
hdb_labels = hdb.fit_predict(X)

# Plotting
fig, axes = plt.subplots(1, 2, figsize=(14, 6))

# Subplot KMeans
for cluster_id in range(k):
    axes[0].scatter(
        X_pca[kmeans_labels == cluster_id, 0],
        X_pca[kmeans_labels == cluster_id, 1],
        label=f"Cluster {cluster_id}"
    )
axes[0].set_title("KMeans Clustering (k=3)")
axes[0].set_xlabel("PCA 1")
axes[0].set_ylabel("PCA 2")
axes[0].legend()
axes[0].grid(True)

# Subplot HDBSCAN
for cluster_id in set(hdb_labels):
    label = f"Cluster {cluster_id}" if cluster_id != -1 else "Noise"
    axes[1].scatter(
        X_pca[hdb_labels == cluster_id, 0],
        X_pca[hdb_labels == cluster_id, 1],
        label=label
    )
axes[1].set_title("HDBSCAN Clustering")
axes[1].set_xlabel("PCA 1")
axes[1].set_ylabel("PCA 2")
axes[1].legend()
axes[1].grid(True)

plt.tight_layout()
plt.show()
