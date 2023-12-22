import sqlite3
import argparse
from main import create_funcdep_table, add_table_content

def display_tables(conn):
    # Ajoutez ici le code pour afficher les tables dans la base de données
    pass

def add_table(conn, table_name):
    create_funcdep_table(conn, table_name)
    print(f"Table '{table_name}' ajoutée avec succès.")

def add_content(conn, table_name, content):
    add_table_content(conn, table_name, content)
    print(f"Contenu ajouté à la table '{table_name}'.")

def main():
    parser = argparse.ArgumentParser(description="Gestion des dépendances fonctionnelles dans une base de données.")
    parser.add_argument("--database", default="oussamabd.db", help="Nom de la base de données SQLite.")
    subparsers = parser.add_subparsers(dest="command", help="Commandes disponibles.")

    # Commande pour afficher les tables
    parser_display = subparsers.add_parser("affiche", help="Afficher les tables dans la base de données.")

    # Commande pour ajouter une nouvelle table
    parser_add_table = subparsers.add_parser("ajouter", help="Ajouter une nouvelle table.")
    parser_add_table.add_argument("table_name", help="Nom de la nouvelle table.")

    # Commande pour ajouter du contenu à une table existante
    parser_add_content = subparsers.add_parser("ajouter_contenu", help="Ajouter du contenu à une table existante.")
    parser_add_content.add_argument("table_name", help="Nom de la table existante.")
    parser_add_content.add_argument("content", nargs="+", help="Contenu à ajouter à la table.")

    args = parser.parse_args()

    try:
        conn = sqlite3.connect(args.database)

        if args.command == "affiche":
            display_tables(conn)
        elif args.command == "ajouter":
            add_table(conn, args.table_name)
        elif args.command == "ajouter_contenu":
            add_content(conn, args.table_name, args.content)
    except sqlite3.Error as e:
        print(f"Erreur SQLite : {e}")
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    main()
