import numpy as np
from sklearn.cluster import MiniBatchKMeans
import joblib
from load_client_data import stream_user_metrics_by_id_range
from load_client_data import build_preprocessor_from_sample
import matplotlib.pyplot as plt
from sklearn.decomposition import PCA
from tqdm import tqdm  

# Paramètres du clustering
n_clusters = 4                                                          # Nombre de clusters souhaités
batch_size = 10000                                                       # Taille des mini-batchs
max_id = 15639775                                                          # Dernier ID utilisateur à traiter
preprocessor = build_preprocessor_from_sample(sample_size=100000)       # Création du préprocessor

# Initialisation du modèle MiniBatchKMeans
kmeans = MiniBatchKMeans(n_clusters=n_clusters, batch_size=batch_size, random_state=0)

# Boucle avec barre de progression
for start_id in tqdm(range(0, max_id, batch_size), desc="Clustering en cours"):
    end_id = min(start_id + batch_size, max_id)

    # Chargement du batch de données via ta fonction
    X = stream_user_metrics_by_id_range(preprocessor, start_id=start_id, end_id=end_id)

    # Entraînement partiel du modèle sur le mini-batch
    if X is not None and len(X) > 0:
        kmeans.partial_fit(X)

# Sauvegarde du modèle entraîné
joblib.dump(kmeans, 'mini_batch_kmeans_model.pkl')
print("Modèle MiniBatchKMeans sauvegardé sous 'mini_batch_kmeans_model.pkl'")


X = stream_user_metrics_by_id_range(preprocessor, start_id=0, end_id=2000)
kmeans_labels = kmeans.predict(X)

# Réduction de dimension pour visualisation
pca = PCA(n_components=2)
X_pca = pca.fit_transform(X)

# Affichage des clusters en 2D
plt.figure(figsize=(8, 6))
scatter = plt.scatter(X_pca[:, 0], X_pca[:, 1], c=kmeans_labels, cmap='viridis', s=10)
plt.title("Clusters visualisés après réduction par PCA (k=4)")
plt.xlabel("Composante principale 1")
plt.ylabel("Composante principale 2")
plt.colorbar(scatter, label='Cluster')
plt.grid(True)
plt.tight_layout()
plt.show()