# Emergency_covid19_datawarehouse
Explorez l'univers dynamique de la santé publique avec notre entrepôt de données, révélant des tendances et des insights cruciaux pour guider les actions.
Une solution puissante pour comprendre, anticiper et lutter - car dans la bataille contre le COVID-19, les données sont votre meilleure arme. 


## Objectif
— Quel est le pourcentage de passages aux urgences pour suspicion de covid-19 par rapport au
nombre de passages total, par mois et par région ?
— Quel est le pourcentage de passages aux urgences pour suspicion de covid-19 par rapport au
nombre de passages total, par tranche d’âge en 2022 ?
— Quel est le pourcentage de passages aux urgences pour suspicion de covid-19 par rapport au
nombre de passages total, pour les personnes agées de plus de 65 ans, en 2023 ?
— Quel est le pourcentage de passages aux urgences pour suspicion de covid-19 par rapport au
nombre de passages total pour les femmes par an et par département ?
— Quel est le pourcentage de passages aux urgences pour suspicion de covid-19 par rapport au
nombre de passages total pour les hommes par an et par département ?
— Quel est le rapport entre le nombre des hospitalisations des hommes et celui des femmes par jour
et par région ?

## Résumé
Notre "Emergency_covid19_datawarehouse" est le résultat d'un processus méthodique visant à dévoiler les tendances du COVID-19. Nous avons conçu un entrepôt qui 
répond aux questions précedemment posées sur la base de différents fichiers sources. Ainsi nous avons automatisé un processus etl afin de charger notre entrepôt 
offrant ainsi un aperçu clair sur le pourcentage de passages aux urgences liés au COVID-19 par région, tranche d'âge, et bien plus. 

## Procédure à suivre pour lancer le DAG

1. **Installer Docker:**
   -  Assurez-vous que Docker et Docker Compose sont installés sur votre machine.
   - Si ce n'est pas le cas, suivez les instructions sur [le site officiel de Docker](https://docs.docker.com/get-docker/) 
     pour installer Docker sur votre machine.

2. **Cloner le Projet**
   - Clonez le projet depuis GitHub si vous ne l'avez pas déjà fait:
     ```bash
     git clone https://github.com/votre-utilisateur/Projet_airflow.git
     ```

3. **Cherchez le repertoire project_airflow`:**
   - Naviguez dans le répertoire principal du projet où se trouve le fichier docker-compose.yaml.
    ```bash
    cd Projet_airflow
    ```

4. **Démarrez le conteneur**
   - Exécutez la commande suivante pour démarrer le conteneur Docker d'Airflow.:
     ```bash
     docker-compose up -d
     ```

5. **Accéder à l'Interface Web Airflow**
   - Ouvrez votre navigateur et accédez à http://localhost:8080. 
   - Connectez-vous à l'interface Airflow avec les identifiants par défaut (username: airflow, password: airflow).

6. **Activez le DAG**
   - Dans l'interface Airflow, accédez à la section "DAGs" et recherchez le DAG "extract_transform". 
   - Activez le DAG en basculant le bouton d'activation.

7. **Démarrez l'Exécution **
   - Cliquez sur le bouton de lecture ou déclenchez manuellement l'exécution du DAG. 

##
