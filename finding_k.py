import matplotlib.pyplot as plt
from sklearn.cluster import KMeans
from load_client_data import stream_user_metrics_by_id_range
from load_client_data import build_preprocessor_from_sample


preprocessor = build_preprocessor_from_sample(sample_size = 100000)

X = stream_user_metrics_by_id_range(preprocessor, start_id=0, end_id=10000)

print(X.head(5))

nan_counts = X.isna().sum()
nan_counts = nan_counts[nan_counts > 0]
print(nan_counts)

inertias = []
k_values = range(1, 15)

for k in k_values:
    kmeans = KMeans(n_clusters=k, random_state=42)
    kmeans.fit(X)
    inertias.append(kmeans.inertia_)
    print(f"Calcul de l'inertie pour k = {k} terminé.\nl'inertie est de {kmeans.inertia_}\n_______________")


plt.plot(k_values, inertias, marker='o')
plt.xlabel('Nombre de clusters (k)')
plt.ylabel('Inertie')
plt.title('Méthode du coude pour déterminer le nombre optimal de clusters')
plt.grid(True)
plt.show()