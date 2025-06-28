dans l'ordre : 

    - create_container_clickhouse
    - create_table
    - traitement_insertion
    - creation_table_client
    - load_client_data
    - finding_k



pour le modele, normalement tu as juste a push load_client_data;py et fit_minibatch_kmeans.py, rentrer les bonnes info de connexion; et lancer fit_minibatch_kmeans.py. Normalement ca devrait marcher!