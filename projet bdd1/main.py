import os
import atexit
from dfControl import *
from DataBaseControl import *

# Global database handler variable
dbh = None
dfh = dfControl("oussamabd.db")

def main_menu():
    while True:
        print("Main Menu:")
        print("1. Add Dependency")
        print("2. Edit Dependency")
        print("3. Delete Dependency")
        print("4. Analyze Dependencies")
        print("5. Add Table") 
        print("6. Drop Table")
        print("7. Exit")

        choice = input("Enter your choice (1-6): ")
        if choice == '1':
            add_dependency()
        elif choice == '2':
            edit_dependency()
        elif choice == '3':
            delete_dependency()
        elif choice == '4':
            analyse_dependencies()
        elif choice == '5':
            add_table() 
        elif choice == '6':
            drop_table()
        elif choice == '7':
            exit_program()
        else:
            print("Invalid choice. Please enter a number between 1 and 6.")



def add_dependency():
    """Add a functional dependency to the FuncDep table."""
    cls()
    dependencies = dbh.get_all_dependencies()

    if len(dependencies) == 0:
        empty_table = input("The FuncDep table currently has no elements.")
        main_menu()
    else:
        print("FuncDep table contains the following dependencies:")
        for dep in dependencies:
            print(f"Table: {dep[0]}  Functional Dependency: {dep[1]} --> {dep[2]}")

    print("-----------------------------------------------")
    print("Enter the following details:")
    table_name = input("Name of the table: ")
    print("Left-hand side of the functional dependency:")
    lhs = input("(if you have multiple elements, separate them with spaces, not commas): ")
    rhs = input("Right-hand side of the functional dependency: ")

    if not dfh.is_dep(table_name, lhs, rhs):
        error_message = input("The dependency is not valid for the chosen table. Please check your input.")
        main_menu()

    # Ajouter la dépendance en spécifiant le nom de la table dans laquelle elle appartient
    added_dependency = dbh.insert_dependency(table_name, lhs, rhs)

    if not added_dependency:
        error_message = input("The dependency could not be added. Please check if the dependency already exists or is correctly written.")
        main_menu()

    print_message = input(f"The dependency has been successfully added: {lhs} --> {rhs}")
    main_menu()


def edit_dependency():
    cls()
    dependencies = dbh.get_all_dependencies()

    if len(dependencies) == 0:
        empty_table = input("La table FuncDep n'a actuellement aucun élément à modifier.")
        main_menu()
    else:
        print("Quelle ligne souhaitez-vous modifier ?")
        increment = 1

        for dep in dependencies:
            print(f"{increment}.  Table: {dep[0]}  Dépendance Fonctionnelle: {dep[1]} --> {dep[2]}")
            increment += 1

        try:
            num = input("Entrez le numéro de la ligne : ")
            nbre = int(num)

            if nbre > len(dependencies) or nbre <= 0:
                error_int = input("Cette ligne n'existe pas.")
                edit_dependency()
            else:
                cls()
                print("Que souhaitez-vous modifier ?")
                print("1. Table")
                print("2. Lhs")
                print("3. Rhs")

                choice = input("Entrez le numéro : ")
                new_nbre = int(choice)
                new_value = input("Entrez la nouvelle valeur : ")

                if new_nbre == 1:
                    retour = dfh.edit_dep(dependencies[nbre - 1][0], dependencies[nbre - 1][1], dependencies[nbre - 1][2], new_value, dfh.TABLE)

                    if retour:
                        print("Vos données ont été modifiées avec succès.")
                        print_dep = input(f"La nouvelle dépendance est : {new_value} {dependencies[nbre - 1][1]} --> {dependencies[nbre - 1][2]}")
                        main_menu()
                    else:
                        error_edit = input("Une erreur s'est produite lors de la modification de votre dépendance.")
                        main_menu()

                elif new_nbre == 2:
                    retour = dfh.edit_dep(dependencies[nbre - 1][0], dependencies[nbre - 1][1], dependencies[nbre - 1][2], new_value, dfh.LHS)

                    if retour:
                        print("Vos données ont été modifiées avec succès.")
                        print_dep = input(f"La nouvelle dépendance est : {dependencies[nbre - 1][0]} {new_value} --> {dependencies[nbre - 1][2]}")
                        main_menu()
                    else:
                        error_edit = input("Une erreur s'est produite lors de la modification de votre dépendance.")
                        main_menu()

                elif new_nbre == 3:
                    if new_value.count(" ") != 0:
                        print("Le nouveau Rhs est incorrectement écrit ; il contient plus d'un élément.")
                        edit_dependency()
                    else:
                        retour = dfh.edit_dep(dependencies[nbre - 1][0], dependencies[nbre - 1][1], dependencies[nbre - 1][2], new_value, dfh.RHS)

                        if retour:
                            print("Vos données ont été modifiées avec succès.")
                            print_dep = input(f"La nouvelle dépendance est : {dependencies[nbre - 1][0]} {dependencies[nbre - 1][1]} --> {new_value}")
                            main_menu()
                        else:
                            error_edit = input("Une erreur s'est produite lors de la modification de votre dépendance.")
                            main_menu()

                else:
                    error_int = input("Le numéro que vous avez entré ne fait pas partie des numéros disponibles.")
                    edit_dependency()

        except ValueError:
            except_error = input("Une erreur s'est produite pendant une opération.")
            edit_dependency()

def delete_dependency():
    cls()
    dependencies = dbh.get_all_dependencies()

    if len(dependencies) == 0:
        empty_table = "The FuncDep table currently has no elements to delete."
        print(empty_table)
        main_menu()
    else:
        print("Which line do you want to delete?")
        increment = 1

        for dep in dependencies:
            print(f"{increment}.  Table: {dep[0]}  Functional Dependency: {dep[1]} --> {dep[2]}")
            increment += 1

        try:
            num = input("Enter the line number: ")
            nbre = int(num)

            if nbre > len(dependencies) or nbre <= 0:
                error_int = "The line you chose does not exist."
                print(error_int)
                delete_dependency()
            else:
                cls()
                verification = input("Deletion is permanent. Do you really want to continue? (Y/N): ")

                if verification == "Y" or verification == "y":
                    if dbh.remove_dependency(dependencies[nbre - 1][0], dependencies[nbre - 1][1], dependencies[nbre - 1][2]):
                        print_dep = input(f"The dependency {dependencies[nbre - 1][1]} --> {dependencies[nbre - 1][2]} from the table {dependencies[nbre - 1][0]} has been successfully deleted.")
                        main_menu()
                    else:
                        error_del = input("An error occurred during the operation.")
                        main_menu()
                elif verification == "N" or verification == "n":
                    main_menu()
                else:
                    error_synth = input("You did not enter the correct character.")
                    delete_dependency()

        except ValueError:
            except_error = input("An error occurred during an operation.")
            delete_dependency()

def drop_table():
    cls()
    tables = dbh.get_table_names()

    if not tables:
        print("No tables available to delete.")
        input("Press Enter to continue...")
        main_menu()

    print("Available tables:")
    for index, table in enumerate(tables, start=1):
        print(f"{index}. {table}")

    try:
        table_index = int(input("Enter the number of the table to delete: "))
        if 1 <= table_index <= len(tables):
            table_to_delete = tables[table_index - 1]
            confirmation = input(f"Are you sure you want to delete the table '{table_to_delete}'? (Y/N): ")

            if confirmation.lower() == 'y':
                dbh.drop_table(table_to_delete)
                print(f"Table '{table_to_delete}' has been successfully deleted.")
            else:
                print("Deletion canceled.")

        else:
            print("Invalid table number.")

    except ValueError:
        print("Invalid input. Please enter a number.")

    input("Press Enter to continue...")
    main_menu()

def add_table():
    cls()
    table_name = input("Enter the name of the new table: ")
    column_count = int(input("Enter the number of columns for the new table: "))

    # Demander les noms des colonnes à l'utilisateur
    column_names = []
    for i in range(column_count):
        column_name = input(f"Enter the name for column {i + 1}: ")
        column_names.append(column_name)

    # Appeler la méthode pour ajouter le tableau avec les colonnes
    if dbh.create_empty_table(table_name, column_names):
        success_message = input(f"The table {table_name} has been successfully added.")
    else:
        error_message = input(f"An error occurred while adding the table {table_name}.")

    main_menu()

def analyse_dependencies():
    cls()
    dependencies = dbh.get_all_dependencies()

    if len(dependencies) == 0:
        empty_table = input("Analysis option temporarily unavailable as the FuncDep table is empty.")
        main_menu()
    else:
        try:
            print("What do you want to do?")
            print("1. Determine the keys of a schema")
            print("2. Determine logical consequences")
            print("3. Unsatisfied DF(s)")
            print("4. BCNF")
            print("5. 3NF")
            print("6. Return to the main menu")

            option = input("Enter the number: ")
            option_number = int(option)

            if option_number == 1:
                try:
                    cls()
                    print("Which table do you want to determine the keys of?")
                    print("-----------------------------------------------")
                    tables = dbh.get_table_names()

                    for table in tables:
                        print(table)
                    print("-----------------------------------------------")

                    table_name = input("Enter the name of the table: ")
                    keys = dfh.get_cle(table_name)

                    if keys is not None:
                        keys_message = input(f"The keys of the table {table_name} are: {keys}")
                        main_menu()
                    else:
                        error_keys = input("An error occurred while determining the keys.")
                        main_menu()

                except ValueError:
                    error_value = input("An error occurred during an operation.")
                    analyse_dependencies()

            elif option_number == 2:
                cls()
                print("Which table do you want to determine the logical consequences of?")
                print("-----------------------------------------------")
                tables = dbh.get_all_tables_in_func_dep()

                for table in tables:
                    print(table)
                print("-----------------------------------------------")

                table_name = input("Enter the name of the table: ")

                # Get all functional dependencies of the chosen table
                dependencies = dbh.get_dependency_by_relation(table_name)
                if  len(dependencies) > 0:
                    print(f"Functional dependencies for the table {table_name}:")
                    for dep in dependencies:
                        print(f"{dep[1]} --> {dep[2]}")

                    # Allow the user to choose a functional dependency
                    selected_dep = input("Choose a functional dependency (enter the number): ")

                    try:
                        dep_index = int(selected_dep)
                        if 1 <= dep_index <= len(dependencies):
                            selected_dependency = dependencies[dep_index - 1]
                            lhs = selected_dependency[1]
                            rhs = selected_dependency[2]

                            # Call the is_logic_consequence function with the correct arguments
                            is_consequence = dfh.is_logic_consequence(table_name, lhs, rhs, False)

                            if is_consequence:
                                print("The selected functional dependency is a logical consequence.")
                            else:
                                print("The selected functional dependency is not a logical consequence.")
                        else:
                            print("Invalid selection. Please enter a valid number.")
                    except ValueError:
                        print("Invalid input. Please enter a number.")
                else:
                    print(f"The table {table_name} has no functional dependencies.")
                    main_menu()

            elif option_number == 3:
                cls()
                print("Which table do you want to determine the unsatisfied dependencies of?")
                print("-----------------------------------------------")
                tables = dbh.get_all_tables_in_func_dep()

                for table in tables:
                    print(table)
                print("-----------------------------------------------")

                table_name = input("Enter the name of the table: ")
                unsatisfied_dependencies = dbh.unsatisfied_dependencies(table_name)

                if unsatisfied_dependencies is not None:
                    if len(unsatisfied_dependencies) == 0:
                        no_unsatisfied_message = input("This table has no unsatisfied dependency.")
                        main_menu()
                    else:
                        unsatisfied_message = input(f"The unsatisfied dependencies of the table {table_name} are: {unsatisfied_dependencies}")
                        main_menu()
                else:
                    error_unsatisfied = input("An error occurred while fetching the unsatisfied dependencies.")
                    main_menu()

            elif option_number == 4:
                analyse_bcnf()

            elif option_number == 5:
                analyse_3nf()

            elif option_number == 6:
                main_menu()

            else:
                error_option = input("The chosen option does not exist.")
                main_menu()

        except ValueError:
            error_value = input("An error occurred during an operation.")
            main_menu()


def analyse_bcnf():
    cls()
    tables = dbh.get_all_tables_in_func_dep()

    print("Which table do you want to determine the BCNF of?")
    print("-----------------------------------------------")

    for table in tables:
        print(table)
    print("-----------------------------------------------")

    table_name = input("Enter the name of the table: ")
    
    if dfh.is_bcnf(table_name) is not None:
        if dfh.is_bcnf(table_name):
            bcnf_message = input(f"The table {table_name} is in BCNF.")
            main_menu()
        else:
            bcnf_message = input(f"The table {table_name} is not in BCNF.")
            main_menu()
    else:
        error_bcnf = input("An error occurred while determining BCNF.")
        main_menu()

def analyse_3nf():
    cls()
    tables = dbh.get_table_names()

    print("Which table do you want to determine the 3NF of?")
    print("-----------------------------------------------")

    for table in tables:
        print(table)
    print("-----------------------------------------------")

    table_name = input("Enter the name of the table: ")
    is_in_3nf = dfh.is_3nf(table_name)

    if is_in_3nf is not None:
        if is_in_3nf:
            tnf_message = input(f"The table {table_name} is in 3NF.")
            main_menu()
        else:
            tnf_message = input(f"The table {table_name} is not in 3NF.")
            main_menu()
    else:
        error_3nf = input("An error occurred while determining 3NF.")
        main_menu()

def initialize_database():
    global dbh
    db_path = "oussamabd.db"
    cls()

    if os.path.exists(db_path):
        print("Loading the database...")
        dbh = DataBaseControl(db_path)
        print("Database successfully loaded.")
    else:
        print("The database does not exist. Creating a new one...")
        dbh = DataBaseControl(db_path)
        print("Database successfully created.")

    main_menu()

def cls():
    os.system('cls' if os.name == 'nt' else 'clear')

def on_close():
    if dbh is not None:
        print("Closing the database connection...")
        dbh.close_connection()
        print("Database connection closed.")

def exit_program():
    on_close()
    print("Exiting the program...")
    exit()

# Register the onclose function
atexit.register(on_close)

# Clear the screen and initialize the application
cls()
print("Welcome to this Application")
initialize_database()
