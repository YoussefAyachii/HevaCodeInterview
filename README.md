# Exercices - Data Management

Ces exercices tournent autour de la manipulation de données dans le but d'évaluer le ou la candidat·e sur ses capacités d'organisation et de réflexion pour répondre à un problème.

Pour ces exercices, nous proposons de travailler sur les données fournies par nos soins qui présentent des enjeux proches de ceux rencontrés en travaillant sur les données du SNDS.
Il s'agit d'évaluation de films par des utilisateurs de la plateforme [IMDB](https://github.com/sidooms/MovieTweetings).

## Préparation de l'environnement et attentes

Nous utiliserons Python en version [3.10](https://www.python.org/downloads/) comme outil de script.
Il est attendu du ou de la candidat·e les fichiers finaux permettant d'obtenir les résultats décrits ci-après.

La base de données est fournie par le fichier SQLite `data/movies.sqlite`.
Deux tables nous intéressent ici : 

 - `movies` : comporte les colonnes `movie_id`, `title` et `genre`
 - `ratings`: comporte les colonnes `user_id`, `movie_id`, `rating` et `rating_timestamp`

L'exploitation de cette base de données peut-être réalisée en utilisant le module `sqlite3` de la bibliothèque standard :

```python
import sqlite3

with sqlite3.connect("data/movies.sqlite") as co:
    co.execute(
            "SELECT rating FROM ratings LIMIT 10"
        ).fetchall()
```

Néanmoins, nous encourageons le ou la candidat·e à utiliser les outils de son choix (exemples : `pandas`, `sqlalchemy`, `jupyter`), tant que le travail est documenté pour la reproductibilité (définition d'un `requirements.txt` à envoyer avec le code source et les résultats).

Par ailleurs, nous attendons que les résultats soient inclus dans un fichier de rapport généré par le code source.
La mise en forme peut être très simple :

```python
nb_films = 10
with open("results.txt", "a", encoding="utf8") as f:
    f.write(f"{nb_films} films figurent dans la base de données.")
```

### Évaluation

L'enjeu de ces exercices est de mettre en valeur votre façon de raisonnner et de vous organiser.
Nous serons particulièrement intéressés par les structures de programmation que vous mettrez en place.
Nous accordons, au sein de notre équipe, une grande importance au fait de pouvoir reprendre le travail d'un collaborateur.
La reproductibilité et la facilité de prise en main de votre travail sera également un critère d'évaluation important.  
En revanche, votre connaissance fine de la grammaire Python, si elle est intéressante, ne sera pas un critère déterminant de ces exercices.

Ainsi, les fichiers sources en retour seront évalués selon les modalités suivantes :

- **Organisation** : comment le problème a-t-il été abordé ?
- **Clean code** : le code source est-il de qualité, bien construit et organisé ? Le travail est-il reproductible et maintenable ?
- **Résultats** : les résultats sont-ils conformes aux attentes ?

## Tâches

### 1. Dénombrements

- 1.1 Combien de films figurent dans la base de données ?
- 1.2 Combien d'utilisateurs différents figurent dans la base de données ?
- 1.3 Quelle est la distribution des notes renseignées ?  
    **Bonus** : créer un histogramme.
- 1.4 Nous souhaitons finalement obtenir une table des fréquences pour exprimer en pourcentage la répartition des notes.

### 2. Sélection et enrichissement des données

- 2.1 Afin de mettre en place un certain modèle statistique, nous devons transformer la note `rating` en deux modalités : l'utilisateur a-t-il aimé ou pas le film ?
    Créer une nouvelle colonne `liked` dans la table `ratings` avec les valeurs suivantes : `0` pour les notes [0-6] et `1` pour les notes [7-10].
- 2.2 Quels sont les genres les mieux notés par les utilisateurs ? Nous souhaitons obtenir le **top 10** des genres de films aimés par les utilisateurs (à l'aide de la nouvelle colonne `liked`).

### Sélections avancées

- 3.1 Quels sont les titres des films les plus aimés des internautes ?  
    Nous cherchons les **10** films les mieux notés en moyenne par les utilisateurs, avec un minimum de **5** notations pour que la mesure soit pertinente.
- 3.2 Quel est le film le plus noté durant l'année 2020 ?  
    **Note** : la colonne `rating_timestamp` est fournie dans la base sous forme d'[heure Unix](https://fr.wikipedia.org/wiki/Heure_Unix).

### Gestion des données

- 4.1 Afin de retrouver plus rapidement les notes d'un utilisateur en particulier, nous souhaitons mettre en place un index sur les id utilisateurs.
    Constatez-vous une différence de performances en recherchant les évaluations données par l'internaute `255` ?
