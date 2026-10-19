# DataLakeHouse

## Objectif

Création d'une plateforme Data agnostique flexible économique gouvernée durable stockage massif pour des usages IA et BI.
## Stack technique

- Moteur de calcul : Trino
- Stockage : RustFS
- Catalogue Data : Polaris
- Format de stockage : IceBerg
- Ingestion Transformation : DBT / Python
- Orchestration : Airflow
- Architecture Data : Medaillon (Bronze/Silver/Gold)
- Gouvernance Data : Open MetaData (Optionnel)
- Extraction de données : Python
- BI : Metabase (Optionnel)

## Environnement 

La solution doit être déployable en local sur mon poste de travail et sur mon VPS OVH.
Mes sources sont stocké sur github.
## Prompt pour le sous projet extraction de données :

*Dans le cadre d'un projet de Data j'aimerai récupérer des données publiques avec Python afin de les ingérer dans la plateforme data et générer un exemple autour de données concernant les traffics ferroviaires de france.

*Peux tu me donner un code python pour l'extraction et un jeu de données OpenSource pour mon premier dashboard
## Execution de l'extraction

```
uvicorn main:app --reload
```

## Prompt pour initialiser la plateforme data

*Peux tu me créer un fichier .yml pour générer l'environnement docker avec les éléments suivant :

- *Moteur de calcul : Trino
- *Stockage : RustFS
- *Catalogue Data : Polaris
- *Format de stockage : IceBerg
- *Ingestion Transformation : DBT / Python
- *Orchestration : Airflow
- *Architecture Data : Medaillon (Bronze/Silver/Gold)