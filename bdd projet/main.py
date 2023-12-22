from dfHandler import *
from DataBaseHandler import *
import os
import atexit

dbh = None

def add_dependency():
    """
    Fonction permettant l'ajout de dépendance fonctionnelle.
    """
    clear_screen()
    dep_table = dbh.getAllDep()

    if not dep_table:
        empty_table = input("La table FuncDep ne contient aucun élément actuellement")
        main_menu()
    else:
        print("La table FuncDep contient les dépendances suivantes : ")
        for line in dep_table:
            print(f"Table: {line[0]}  Dépendance fonctionnelle: {line[1]} --> {line[2]}")

    print("-----------------------------------------------")
    print("Entrez les éléments suivants :")
    add_tables = input("Nom de la table : ")
    print("Partie gauche de la dépendance fonctionnelle :")
    add_lhs = input("(si vous avez plusieurs éléments, séparez-les par des espaces et non des virgules) : ")
    add_rhs = input("Partie droite de la dépendance fonctionnelle : ")

    dep = dbh.insertDep(add_tables, add_lhs, add_rhs)

    if not dep:
        error_add = input("Votre dépendance n'a pas pu être ajoutée. Veuillez vérifier que la dépendance n'existe pas déjà ou a été bien écrite.")
        main_menu()

    print_dep = input(f"Votre dépendance a bien été ajoutée : {add_lhs} --> {add_rhs}")
    main_menu()

def edit_dependency():
    """
    Fonction permettant la modification de dépendance fonctionnelle.
    """
    clear_screen()
    dep_table = dbh.getAllDep()

    if not dep_table:
        empty_table = input("La table FuncDep ne contient aucun élément à modifier")
        main_menu()
    else:
        print("Quelle ligne voulez-vous modifier?")
        increment = 1
        for line in dep_table:
            print(f"{increment}.  Table: {line[0]}  Dépendance fonctionnelle: {line[1]} --> {line[2]}")
            increment += 1

        try:
            num = input("Entrez le numéro de la ligne : ")
            nbre = int(num)

            if nbre > len(dep_table) or nbre <= 0:
                error_int = input("Cette ligne n'existe pas")
                edit_dependency()
            else:
                clear_screen()
                print("Que voulez-vous modifier?")
                print("1. Table")
                print("2. Lhs")
                print("3. Rhs")

                choice = input("Entrez le nombre : ")
                new_nb = int(choice)
                new_data = input("Entrez les nouvelles données :")

                if new_nb == 1:
                    attribute = dbh.TABLE
                elif new_nb == 2:
                    attribute = dbh.LHS
                elif new_nb == 3:
                    if new_data.count(" ") != 0:
                        print("Le nouvel Rhs est mal écrit, il contient plus d'un élément")
                        edit_dependency()
                    attribute = dbh.RHS
                else:
                    error_int = input("Le nombre que vous avez inséré ne fait pas partie des nombres disponibles")
                    edit_dependency()

                return_value = dbh.editDep(dep_table[nbre - 1][0], dep_table[nbre - 1][1], dep_table[nbre - 1][2], new_data, attribute)

                if return_value:
                    print("Votre donnée a bien été modifiée")
                    print_dep = input(f"La nouvelle dépendance est : {dep_table[nbre - 1][1]} --> {dep_table[nbre - 1][2]}")
                    main_menu()
                else:
                    error_edit = input("Une erreur est apparue lors de la modification de votre dépendance")
                    main_menu()

        except ValueError:
            except_error = input("Une erreur s'est produite lors d'une opération")
            edit_dependency()

def delete_dependency():
    """
    Fonction permettant la suppression de dépendance.
    """
    clear_screen()
    dep_table = dbh.getAllDep()

    if not dep_table:
        empty_table = input("La table FuncDep ne contient aucun élément à supprimer")
        main_menu()
    else:
        print("Quelle ligne voulez-vous supprimer?")
        increment = 1
        for line in dep_table:
            print(f"{increment}.  Table: {line[0]}  Dépendance fonctionnelle :{line[1]} --> {line[2]}")
            increment += 1

        try:
            num = input("Entrez le numéro de la ligne : ")
            nbre = int(num)

            if nbre > len(dep_table) or nbre <= 0:
                error_int = input("La ligne que vous avez choisie n'existe pas")
                delete_dependency()
            else:
                clear_screen()
                verif = input("La suppression est définitive, voulez-vous vraiment continuer ? (Y/N) : ")

                if verif.lower() == "y":
                    if dbh.removeDep(dep_table[nbre - 1][0], dep_table[nbre - 1][1], dep_table[nbre - 1][2]):
                        print_dep = input(f"La dépendance {dep_table[nbre - 1][1]} --> {dep_table[nbre - 1][2]} venant de la table {dep_table[nbre - 1][0]} a bien été supprimée")
                        main_menu()
                    else:
                        error_del = input("Une erreur s'est produite pendant l'opération")
                        main_menu()
                elif verif.lower() == "n":
                    main_menu()
                else:
                    error_synth = input("Vous n'avez pas entré le bon caractère")
                    delete_dependency()

        except ValueError:
            except_error = input("Une erreur s'est produite lors d'une opération")
            delete_dependency()
