import random
import string

# 1. Configuration de la session Spark
# Assurez-vous que votre cluster a suffisamment de ressources pour cette opération !
spark = SparkSession.builder \
    .appName("MassiveDataGenerator") \
    .config("spark.sql.shuffle.partitions", "200") \
    .getOrCreate()

# --- Paramètres de la génération ---
# Cible : 1,2 Milliards de lignes
TARGET_ROWS = 1_200_000_000
NUM_COLUMNS = 15
OUTPUT_PATH = "s3a://votre-bucket/massive_test_data_parquet" # Adaptez à votre chemin (S3, HDFS, local, etc.)

# Définition des fonctions de génération de données
def generate_random_string(length=10):
    """Génère une chaîne aléatoire de la longueur spécifiée."""
    characters = string.ascii_letters + string.digits
    return ''.join(random.choice(characters) for i in range(length))

def generate_random_date():
    """Génère une date aléatoire entre 2000 et 2025."""
    start_ts = spark.sql("SELECT CAST('2000-01-01' AS DATE)").collect()[0][0]
    end_ts = spark.sql("SELECT CAST('2025-12-31' AS DATE)").collect()[0][0]
    return spark.sql(f"SELECT date_add('{start_ts}', floor(rand() * datediff('{end_ts}', '{start_ts}')))")

# 2. Création du DataFrame de base avec le nombre de lignes requis
# Nous utilisons 'range' pour générer rapidement les identifiants
base_df = spark.range(TARGET_ROWS).repartition(200) # Re-partitionnez pour le parallélisme

# Renommer la colonne ID de base
base_df = base_df.withColumnRenamed("id", "row_id")

# 3. Ajout des 15+ colonnes synthétiques
df_massive = base_df

# Ajout de colonnes numériques et booléennes
df_massive = df_massive.withColumn("col_int_1", (rand() * 10000).cast("int"))
df_massive = df_massive.withColumn("col_double_2", rand() * 99999.99)
df_massive = df_massive.withColumn("col_bool_3", (rand() > 0.5))

# Ajout de colonnes de chaîne (simulant des données catégorielles ou des textes)
for i in range(4, NUM_COLUMNS + 1):
    # Simuler des chaînes de longueur variable
    if i % 3 == 0: # Colonne de chaîne courte (e.g., code pays)
         df_massive = df_massive.withColumn(f"col_string_{i}", lit("CODE_" + (rand() * 100).cast("int").cast("string")))
    elif i % 3 == 1: # Colonne de chaîne moyenne (e.g., nom)
         df_massive = df_massive.withColumn(f"col_string_{i}", lit(generate_random_string(15))) # Note: 'lit' ici est statique pour l'efficacité, utiliser une UDF si les valeurs DOIVENT changer par ligne.
    else: # Colonne de chaîne longue
         df_massive = df_massive.withColumn(f"col_text_{i}", lit(generate_random_string(50)))

# Ajout d'une colonne de date/timestamp
df_massive = df_massive.withColumn("col_timestamp_16", (rand() * 1_767_216_000_000).cast("timestamp"))

# Affichage du schéma et du nombre de lignes pour vérification
print(f"Nombre de lignes ciblées : {df_massive.count()}")
df_massive.printSchema()

# 4. Sauvegarde du DataFrame
# Le format Parquet est généralement préféré pour les tests Spark car il est optimisé (colonnaire)
# Sauvegarde en mode "overwrite" pour réexécuter si besoin
df_massive.write \
    .mode("overwrite") \
    .parquet(OUTPUT_PATH)

# Arrêt de la session Spark
spark.stop()
print(f"Génération terminée. Données sauvées dans : {OUTPUT_PATH}")
