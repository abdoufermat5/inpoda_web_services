from zeep import Client, Settings


def getZeepClient(url):
    settings = Settings(strict=False, xml_huge_tree=True)
    return Client(url, settings=settings)


FILE_PATH = '../data/versailles_tweets_100.json'


def loadJsonFile(path):
    import json

    print("Publication d'un ensemble de tweets...")
    json_file = open(path, encoding="UTF-8")
    json_str = json_file.read()
    json_data = json.loads(json_str)

    return json_data


# Console menu for preprocessing service
def preprocessingMenu():

    print(" ------------- BIENVENUE DANS LA CONSOLE DE PREPROCESSING DE DONNÉES  --------------")
    print("-" * 42)
    print("\nLes différentes fonctionnalités:")
    print("0  . Publier un ensemble de tweets")
    print("1  . Identification auteur de la publication")
    print("2  . Extraction des hashtags")
    print("3  . Analyse de sentiment")
    print("4  . Identification du/des topic")
    print("-1 . Quitter")


# Console menu for analysis service
def analysisMenu():
    print(" ------------- BIENVENUE DANS LA CONSOLE D'ANALYSE DE DONNÉES  --------------")
    print("-" * 42)
    print("\nLes différentes fonctionnalités:")
    print("1  . Top K hashtags")
    print("2  . Top K utilisateurs")
    print("3  . Top K topics")
    print("4  . Nombre de publications par utilisateur")
    print("5  . Nombre de publications par hashtag")
    print("6  . Nombre de publications par topic")
    print("-1 . Quitter")


def main_menu():

    print("------------- BIENVENUE DANS LA CONSOLE DE CONSOMMATION DE SERVICES  --------------")
    print("-" * 42)
    print("\nLes différents services:")
    print("1  . Service de preprocessing de données")
    print("2  . Service d'analyse de données")
    print("-1 . Quitter")
