import sqlite3

class DataBaseControl:
    """Database Manager"""

    def __init__(self, database):
        """Constructor for DatabaseHandler"""
        self.db = sqlite3.connect(database)
        self.cursor = self.db.cursor()
        self.cursor.execute(
            """CREATE TABLE IF NOT EXISTS FuncDep('table_name' TEXT NOT NULL, lhs TEXT NOT NULL, rhs TEXT NOT NULL, PRIMARY KEY('table_name', lhs, rhs))"""
        )
        self.db.commit()

    @staticmethod
    def check_data_types(*param):
        """
        Raise TypeError if any of the parameters is not of type str
        """
        for item in param:
            if not isinstance(item, str):
                raise TypeError("Type of parameter is not str")

    def close_connection(self):
        if self.db:
            self.db.close()
            print("Database connection closed.")
        else:
            print("No active database connection.")

    def insert_dependency(self, table_name, lhs, rhs):
        DataBaseControl.check_data_types(table_name, lhs, rhs)
        self.cursor.execute(
            """INSERT INTO FuncDep('table_name', lhs, rhs) VALUES(?, ?, ?)""",
            (table_name, lhs, rhs),
        )
        self.db.commit()
        return self.get_dependency(table_name, lhs, rhs)

    def remove_dependency(self, table_name, lhs, rhs):
        DataBaseControl.check_data_types(table_name, lhs, rhs)
        self.cursor.execute(
            """DELETE FROM FuncDep WHERE FuncDep.'table_name'=? AND lhs=? AND rhs=?""",
            (table_name, lhs, rhs),
        )
        self.db.commit()

    @staticmethod
    def __dataOK(*param):
        """
        Raise TypeError if any of the parameters is not of type str
        """
        for item in param:
            if type(item) != str:
                raise TypeError("Le type du parametre n'est pas str")

    def get_all_lhs(self, table_name):
        DataBaseControl.__dataOK(table_name)
        """
        retourne tous les lhs pour une table donnee
        Param:
            table: la table pour laquelle les lhs seront retournes
        """
        self.cursor.execute(
            """ SELECT lhs FROM FuncDep WHERE FuncDep.'table_name' == ? """, (table_name,)
        )
        return [data[0] for data in self.cursor.fetchall()]

    def get_all_rhs(self, table_name):
        DataBaseControl.__dataOK(table_name)
        """
        retourne tous les lhs pour une table donnee
        Param:
            table: la table pour laquelle les lhs seront retournes
        """
        self.cursor.execute(
            """ SELECT rhs FROM FuncDep WHERE FuncDep.'table_name' == ? """, (table_name,)
        )
        return [data[0] for data in self.cursor.fetchall()]

    def edit_table_dependency(self, table_name, lhs, rhs, new_table):
        DataBaseControl.check_data_types(table_name, lhs, rhs, new_table)
        self.cursor.execute(
            """UPDATE FuncDep SET 'table_name'=? WHERE 'table_name'=? AND lhs=? AND rhs=?""",
            (new_table, table_name, lhs, rhs),
        )
        self.db.commit()

    def edit_lhs_dependency(self, table_name, lhs, rhs, new_lhs):
        DataBaseControl.check_data_types(table_name, lhs, rhs, new_lhs)
        self.cursor.execute(
            """UPDATE FuncDep SET lhs=? WHERE 'table_name'=? AND lhs=? AND rhs=?""",
            (new_lhs, table_name, lhs, rhs),
        )
        self.db.commit()

    def edit_rhs_dependency(self, table_name, lhs, rhs, new_rhs):
        DataBaseControl.check_data_types(table_name, lhs, rhs, new_rhs)
        self.cursor.execute(
            """UPDATE FuncDep SET rhs=? WHERE 'table_name'=? AND lhs=? AND rhs=?""",
            (new_rhs, table_name, lhs, rhs),
        )
        self.db.commit()

    def get_table_names(self):
        self.cursor.execute("""SELECT name FROM sqlite_master WHERE type='table'""")
        return [data[0] for data in self.cursor]

    def create_empty_table(self, table_name, attributes):
        """
        Create a new empty table with the specified attributes.
        Param:
            table_name: the name of the new table
            attributes: array of attributes for the new table
        """
        s = f"CREATE TABLE {table_name} ("
        
        # Loop through each attribute
        for attribute in attributes:
            s += f"{attribute} TEXT, "  # Assuming TEXT data type, you can adjust it based on your requirements
        
        # Remove the trailing comma and space
        s = s[0:len(s) - 2]
        s += ")"
        
        # Execute the CREATE TABLE statement
        self.cursor.execute(s)

    def add_table(self, table_name, column_names):
        try:
            # Construire la requête SQL pour créer une nouvelle table avec les colonnes
            columns = ', '.join([f"{column} VARCHAR(255)" for column in column_names])
            query = f"CREATE TABLE {table_name} ({columns});"
            
            # Exécuter la requête SQL
            self.cursor.execute(query)
            
            # Commit pour sauvegarder les modifications
            self.connection.commit()
            
            return True
        except Exception as e:
            print(f"Error adding table: {e}")
            return False

    def get_table_attributes(self, table_name):
        DataBaseControl.__dataOK(table_name)
        """ 
		Retourne les attributs d'une table de la base de donnee

			Param: tableName: le nom de la table
			Return: Un tableau contenant le noms des attributs de la table TableName

		"""
        retour = []
        s = f"""PRAGMA table_info({table_name})"""
        self.cursor.execute(s)
        for rows in self.cursor:
            retour.append(rows[1])
        return retour

    def get_all_tables_in_func_dep(self):
        """
		retourne tous les noms de tables presentes dans la table FuncDep
		"""
        self.cursor.execute(""" SELECT DISTINCT FuncDep.'table_name' FROM FuncDep""")
        retour = []
        for item in self.cursor:
            retour.append(item[0])
        return retour

    def get_dependency(self, table_name, lhs, rhs):
        query = "SELECT * FROM FuncDep WHERE table_name = ? AND lhs = ? AND rhs = ?"
        self.cursor.execute(query, (table_name, lhs, rhs))
        result = self.cursor.fetchone()
        return list(result) if result is not None else None

    def close_database(self):
        """
        Close the database
        """
        self.db.close()
        self.cursor = None

    def get_all_dependencies(self):
        """
        Return all dependencies from the FuncDep table
        """
        self.cursor.execute("""SELECT * FROM FuncDep""")
        return [list(item) for item in self.cursor.fetchall()]

    def get_dependency_by_relation(self, relation):
        DataBaseControl.check_data_types(relation)
        """
        Return all dependencies associated with the given table
        """
        self.cursor.execute("""SELECT * FROM FuncDep WHERE table_name=?""", (relation,))
        return [list(tuples) for tuples in self.cursor.fetchall()]

    def get_metadata_of_attribute(self, table_name, attribute_name):
        """
        Return the characteristics (cid, tableName, type, not null, default value, primary key)
        of a given attribute from a given table
        Param:
            table: the table containing the attribute
            attribute_name: the attribute for which characteristics are required
        """
        s = f"""PRAGMA table_info({table_name})"""
        caracts = self.cursor.execute(s)
        for caract in self.cursor:
            if caract[1] == attribute_name:
                return caract
        return None

    def create_table(self, table_name, attribute, old_table_name):
        """
        Create a new table and insert data from corresponding attributes
        Param:
            table_name: the name of the new table
            attribute: array of attributes for the new table
            old_table_name: the name of the old table for each attribute in attribute
        """
        data_to_add = None
        s = f"CREATE TABLE {table_name}( "
        for index in range(0, len(attribute)):
            old_table_name_i = old_table_name[index]
            attribute_name = attribute[index]

            if old_table_name_i != 'FuncDep':
                select = f"SELECT {attribute_name} FROM {old_table_name_i}"
                self.cursor.execute(select)
                result_select = self.cursor.fetchall()
                if data_to_add is None:
                    data_to_add = result_select
                else:
                    for i in range(0, len(data_to_add)):
                        data_to_add[i] = data_to_add[i] + result_select[i]

            info = self.get_metadata_of_attribute(old_table_name_i, attribute_name)
            s = s + f"{attribute_name} {info[2]} "
            if info[3] == 1:
                s = s + "NOT NULL" + " "
            if info[4] is not None:
                s = s + f"DEFAULT {info[4]} "
            s += ", "
        s = s[0:len(s) - 2]
        s += ")"
        self.cursor.execute(s)
        values = ""
        s2 = f"INSERT INTO {table_name}( "
        for att in attribute:
            s2 = s2 + f"{att}, "
            values = values + "?, "
        s2 = s2[0:len(s2) - 2]
        values = values[0:len(values) - 2]
        s2 += f") VALUES({values})"
        for line in data_to_add:
            self.cursor.execute(s2, line)

    def remove_old_table(self, old_table_name):
        """
        Remove tables from the old_table_name list
        Param:
            old_table_name: array of names of tables to remove
        """
        for old_table in old_table_name:
            self.drop_table(old_table)

    def drop_table(self, table_name):
        """
        Remove the 'table' table from the database
        Param:
            table: the name of the table to remove
        """
        s_remove = f"DROP TABLE IF EXISTS {table_name}"
        self.cursor.execute(s_remove)
        self.db.commit()
