from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
import json
import csv
import pandas as pd
import os

# Chemin du DAG
dag_path = os.getcwd()

# ici on définit les arguments par défaut pour le DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    
}

# Configuration du DAG
dag = DAG(
    'transform_and_load',
    default_args=default_args,
    description='DAG pour transformer et charger les données',
    schedule_interval=None,  
    catchup=False,
)

# Fonction de transformation des fichiers csv
def transform_csv_data():
    emergency = pd.read_csv(f"{dag_path}/data/donnees-urgences-SOS-medecins.csv", sep = ';', encoding= 'utf-8', low_memory=False)
    age_group = pd.read_csv(f"{dag_path}/data/code-tranches-dage-donnees-urgences.csv", sep = ';', encoding= 'utf-8', low_memory=False)
    # on supprime les lignes en double 
    emergency = emergency.drop_duplicates()
    
    # on supprime les colonnes non nécessaires
    drop_columns = ['nbre_acte_corona', 'nbre_acte_tot', 'nbre_acte_corona_h', 'nbre_acte_corona_f', 'nbre_acte_tot_h', 'nbre_acte_tot_f']
    emergency = emergency.drop(drop_columns, axis=1, errors="ignore")

    # on remplace les valeurs nulles par zéro dans les colonnes appropriées
    emergency['nbre_pass_corona_h'] = emergency['nbre_pass_corona_h'].fillna(0)
    emergency['nbre_pass_corona_f'] = emergency['nbre_pass_corona_f'].fillna(0)
    emergency['nbre_pass_tot_h'] = emergency['nbre_pass_tot_h'].fillna(0)
    emergency['nbre_pass_tot_f'] = emergency['nbre_pass_tot_f'].fillna(0)
    emergency['nbre_hospit_corona_h'] = emergency['nbre_hospit_corona_h'].fillna(0)
    emergency['nbre_hospit_corona_f'] = emergency['nbre_hospit_corona_f'].fillna(0)

    # on sauvegarde nos données nettoyées dans un nouveau répertoire
    emergency.to_csv(f"{dag_path}/cleaned_data/emergency.csv", index=False)
    age_group.to_csv(f"{dag_path}/cleaned_data/age_group.csv", index=False)

# Fonction de transformation du fichier JSON
def transform_json_data():
    with open(f"{dag_path}/data/departements-region.json", "r") as file:
        department = json.load(file)

    # Écrire les données dans le fichier CSV
    with open(f"{dag_path}/cleaned_data/department.csv", 'w', newline='') as final_department:
        csv_writer = csv.DictWriter(final_department, fieldnames=department[0].keys())
        csv_writer.writeheader()
        csv_writer.writerows(department)

# Fonction de création de tables et chargement des données
def load_data():
    # on utilise PostgresHook pour la connexion à notre base de données
    postgres_hook = PostgresHook(postgres_conn_id="postgres_conn")

    # Creation de la table emergency
    create_emergency_table = '''
        CREATE TABLE IF NOT EXISTS emergency (
            id SERIAL PRIMARY KEY,
            date_passage DATE,
            num_dep INTEGER ,
            code_age INTEGER,
            nbre_pass_corona INTEGER,
            nbre_pass_tot INTEGER,
            nbre_hospit_corona INTEGER,
            nbre_pass_corona_h INTEGER,
            nbre_pass_corona_f INTEGER,
            nbre_pass_tot_h INTEGER,
            nbre_pass_tot_f INTEGER,
            nbre_hospit_corona_h INTEGER,
            nbre_hospit_corona_f INTEGER
        );
    '''
    postgres_hook.run(create_emergency_table)

    # on alimente la table emergency avec les données du fichier emergency.csv
    emergency_file = pd.read_csv(f"{dag_path}/cleaned_data/emergency.csv")
    emergency_file.to_sql('emergency', postgres_hook.get_sqlalchemy_engine(), if_exists='replace', index=False)


    # Création de la table age_group 
    create_age_group_table = '''
        CREATE TABLE IF NOT EXISTS age_group (
            code_age INTEGER PRIMARY KEY,
            description VARCHAR(255)
        );
    '''
    postgres_hook.run(create_age_group_table)

    # on alimente la table age_group avec les données du fichier age_group.csv
    age_group_file = pd.read_csv(f"{dag_path}/cleaned_data/age_group.csv")
    age_group_file.to_sql('age_group', postgres_hook.get_sqlalchemy_engine(), if_exists='replace', chunksize=1000)

    # Creation de la table calendar
    create_calendar_table = '''
        CREATE TABLE IF NOT EXISTS calendar (
            date_passage DATE PRIMARY KEY,
            jour INTEGER,
            mois INTEGER,
            libelle_mois VARCHAR(255),
            annee INTEGER      
        );
    '''
    postgres_hook.run(create_calendar_table)

    # on alimente la table avec la colonne date_passage de la table emergency
    insert_into_calendar = '''
        INSERT INTO calendar (date_passage, jour, mois, libelle_mois, annee)
        SELECT
            date_passage,
            EXTRACT(day FROM date_passage) AS jour,
            EXTRACT(month FROM date_passage) AS mois,
            TO_CHAR(date_passage, 'Month') AS libelle_mois,
            EXTRACT(year FROM date_passage) AS annee
        FROM emergency;
    '''
    postgres_hook.run(insert_into_calendar)

    # creation table department
    create_department_table = '''
        CREATE TABLE IF NOT EXISTS department (
            num_dep INT PRIMARY KEY,
            departement VARCHAR(4000),
            region VARCHAR(4000)
        );
    '''
    postgres_hook.run(create_department_table)

    department_file = pd.read_csv(f"{dag_path}/cleaned_data/departement.csv")
    department_file.to_sql('department', postgres_hook.get_sqlachemy_engine(), if_exists= 'replace', index=False)
    


# on instancie nos différents opérateurs 
transform_csv_data_op = PythonOperator(
    task_id='transform_csv_data',
    python_callable=transform_csv_data,
    dag=dag,
)

transform_json_data_op = PythonOperator(
    task_id='transform_json_data',
    python_callable=transform_json_data,
    dag=dag,
)

load_data_op = PythonOperator(
    task_id='load_data',
    python_callable=load_data,
    dag=dag,
)

# on définit les dépendances entre nos tâches
transform_csv_data_op >> load_data_op
transform_json_data_op >> load_data_op




