import sqlite3

class DataBaseHandler:
    """Gestionnaire de base de données"""

    def __init__(self, dataBase):
        """Constructeur de DataBaseHandler"""
        self.db = sqlite3.connect(dataBase)
        self.cursor = self.db.cursor()
        self.cursor.execute("""CREATE TABLE IF NOT EXISTS FuncDep('table' TEXT NOT NULL, lhs TEXT NOT NULL, rhs TEXT NOT NULL, PRIMARY KEY('table', lhs, rhs))""")
        self.db.commit()

    @staticmethod
    def __dataOK(*param):
        """
        Lève une exception de type TypeError si l'un des paramètres n'est pas du type str
        """
        for item in param:
            if type(item) != str:
                raise TypeError("Le type du paramètre n'est pas str")

    def insertDep(self, table, lhs, rhs):
        DataBaseHandler.__dataOK(table, lhs, rhs)
        """ 
        Insère une DF dans la table FuncDep

        Paramètres :
            table : la table à laquelle appartient la DF
            lhs : le membre de gauche de la DF
            rhs : le membre de droite de la DF 

        Retourne :
            La DF insérée
        """
        self.cursor.execute("""INSERT INTO FuncDep('table', lhs, rhs) VALUES (?, ?, ?)""", (table, lhs, rhs))
        self.db.commit()
        self.cursor.execute("""SELECT * FROM FuncDep WHERE Funcdep.'table' = ? AND lhs = ? AND rhs = ? """, (table, lhs, rhs))
        tab = []
        retour = self.cursor.fetchone()
        for item in retour:
            tab.append(item)
        return tab

    def removeDep(self, table, lhs, rhs):
        DataBaseHandler.__dataOK(table, lhs, rhs)
        """ 
        Supprime une DF de la table FuncDep

        Paramètres :
            table : la table à laquelle appartient la DF
            lhs : le membre de gauche de la DF
            rhs : le membre de droite de la DF 
        """
        self.cursor.execute("""DELETE FROM FuncDep WHERE FuncDep.'table' = ? AND lhs = ? AND rhs = ?""", (table, lhs, rhs))
        self.db.commit()

    def editTableDep(self, table, lhs, rhs, newTable):
        DataBaseHandler.__dataOK(table, lhs, rhs, newTable)
        """ 
        Modifie la table d'une DF dans la table FuncDep

        Paramètres :
            table : la table à laquelle appartient la DF
            lhs : le membre de gauche de la DF
            rhs : le membre de droite de la DF
            newTable : le nom de la nouvelle table 

        Retourne :
            La DF modifiée (TODO)
        """
        self.cursor.execute("""UPDATE FuncDep SET Funcdep.'table' = ? WHERE Funcdep.'table' = ? AND lhs = ? AND rhs = ?""", (newTable, table, lhs, rhs))
        self.db.commit()

    def ediLhsDep(self, table, lhs, rhs, newLhs):
        DataBaseHandler.__dataOK(table, lhs, rhs, newLhs)
        """ 
        Modifie la partie de gauche d'une DF dans la table FuncDep

        Paramètres :
            table : la table à laquelle appartient la DF
            lhs : le membre de gauche de la DF
            rhs : le membre de droite de la DF
            newLhs : le nom de la nouvelle partie de gauche de la DF 

        Retourne :
            La DF modifiée (TODO)
        """
        self.cursor.execute("""UPDATE FuncDep SET lhs = ? WHERE Funcdep.'table' = ? AND lhs = ? AND rhs = ?""", (newLhs, table, lhs, rhs))
        self.db.commit()

    def editRhsDep(self, table, lhs, rhs, newRhs):
        DataBaseHandler.__dataOK(table, lhs, rhs, newRhs)
        """ 
        Modifie la partie de droite d'une DF dans la table FuncDep

        Paramètres :
            table : la table à laquelle appartient la DF
            lhs : le membre de gauche de la DF
            rhs : le membre de droite de la DF
            newRhs : le nom de la nouvelle partie de droite de la DF 

        Retourne :
            La DF modifiée (TODO)
        """
        self.cursor.execute("""UPDATE FuncDep SET rhs = ? WHERE Funcdep.'table' = ? AND lhs = ? AND rhs = ?""", (newRhs, table, lhs, rhs))
        self.db.commit()

    def getTableName(self):
        """ 
        Retourne le nom de toutes les tables de la base de données

        Retourne :
            Un tableau contenant les noms de toutes les tables de la base de données
        """
        self.cursor.execute("""SELECT name FROM sqlite_master WHERE type = 'table' """)
        retour = []
        for data in self.cursor:
            retour.append(data[0])
        return retour

    def getTableAttribute(self, tableName):
        DataBaseHandler.__dataOK(tableName)
        """ 
        Retourne les attributs d'une table de la base de données

        Paramètres :
            tableName : le nom de la table

        Retourne :
            Un tableau contenant les noms des attributs de la table tableName
        """
        retour = []
        s = """PRAGMA table_info("""
        s += tableName
        s += """)"""
        self.cursor.execute(s)
        for rows in self.cursor:
            retour.append(rows[1])
        return retour

    def getOneDep(self, table, lhs, rhs):
        DataBaseHandler.__dataOK(table, lhs, rhs)
        """
        Retourne une DF si elle existe. Sinon, retourne un tableau vide

        Paramètres :
            table : la table à laquelle appartient la DF
            lhs : le membre de gauche de la DF
            rhs : le membre de droite de la DF

        Retourne :
            Un tableau contenant la DF. Si elle n'existe pas, le tableau est vide
        """
        self.cursor.execute("""SELECT * FROM FuncDep WHERE FuncDep.'table' = ? AND lhs = ? AND rhs = ? """, (table, lhs, rhs))
        retour = []
        result = self.cursor.fetchone()
        if result == None:
            return None
        else:
            for item in result:
                retour.append(item)
            return retour

    def getDepByRelation(self, relation):
        DataBaseHandler.__dataOK(relation)
        """ 
        Retourne toutes les DF d'une table

        Param: relation un nom d'une table de la base de donnee
        Return: un tableau contenant toutes les DFs associees a la table relation

        """
        self.cursor.execute("""SELECT * FROM FuncDep WHERE FuncDep.'table'=?  """, (relation,))
        retour = [list(tuples) for tuples in self.cursor]
        return retour

    def closeDataBase(self):
        """
        Ferme la base de donnee
        """
        self.db.close()
        self.cursor = None

    def getAllDep(self):
        """ 
        Retourne toutes les DF de la table FuncDep

        Return: Un tableau a deux dimensions ou chaque ligne est une DF

        """
        self.cursor.execute(""" SELECT * FROM FuncDep""")
        retour = [list(items) for items in self.cursor]
        return retour

    def DFisOk(self, table, lhs, rhs):
        DataBaseHandler.__dataOK(table, lhs, rhs)
        """
        retoune les tuples de la table table qui ne respectent pas la df lhs--> rhs
        lhs est un tuple d'attributs et rhs un str ne contenant qu'un attribut
        Param: 
            table: la table a laquelle appartient la DF
            lhs: le membre de gauche de la DF
            rhs: lemembre de droite de la DF
        """

        if isinstance(lhs, str):
            lhsTab = lhs.split()

        s = "SELECT t1.*, t2." + rhs + " FROM " + table + " t1, " + table + " t2 WHERE "
        for attribute in lhsTab:
            s += "t1." + attribute + " == t2." + attribute + " AND "
        s += "t1." + rhs + " != t2." + rhs
        self.cursor.execute(s)

        retour = [list(tuples)[:-1] for tuples in self.cursor]
        return retour

    def getAllLhs(self, table):
        DataBaseHandler.__dataOK(table)
        """
        retourne tous les lhs pour une table donnee
        Param:
            table: la table pour laquelle les lhs seront retournes
        """

        self.cursor.execute(""" SELECT lhs FROM FuncDep WHERE FuncDep.'table' == ? """ ,(table,))
        retour = [item[0] for item in self.cursor]
        return retour

    def getAllRhs(self, table):
        DataBaseHandler.__dataOK(table)
        """
        retourne tous les lhs pour une table donnee
        Param:
            table: la table pour laquelle tous les rhs seront retournes
        """

        self.cursor.execute(""" SELECT rhs FROM FuncDep WHERE FuncDep.'table' == ? """ ,(table,))
        retour = [item[0] for item in self.cursor]
        return retour

    def getAllTableInFuncDep(self):
        """
        retourne tous les noms de tables presentes dans la table FuncDep
        """
        self.cursor.execute(""" SELECT DISTINCT FuncDep.'table' FROM FuncDep""")
        retour = [item[0] for item in self.cursor]
        return retour

    def metadataOfAttribute(self, table, attributeName):
        """
        Rretourne les caracteristiques (cid, tableName, type, not null, default value, primary key ) d'un attribut donne d'une table donnee
        Param:
            table: la table qui contient l'attribut
            attributeName: l'attribut pour lequel on souhaite obtenir les caracteristiques
        """
        s = """PRAGMA table_info(""" + table + """)"""
        self.cursor.execute(s)
        for caract in self.cursor:
            if caract[1] == attributeName:
                return caract
        return None

    def createTable(self, tableName, attribute, oldTableName):
        """
        Cree un nouvelle table et y insere les donnees des attributs correspondants
        Param:
            tableName: le nom de la nouvelle table
            attribute: tableau des attributs de la nouvelle table
            oldTableName: le nom de l'ancienne table pour chaque attributs de attribute
        """
        dataToAdd = None  # tableau contenant les lignes (tuples) a ajouter a la table
        s = "CREATE TABLE " + tableName + "( "
        for index in range(len(attribute)):
            oldTableNameI = oldTableName[index]
            attributeName = attribute[index]

            # on selectionne les donnees a ajouter et on l'ajoute a dataToAdd
            if oldTableNameI != 'FuncDep':
                select = "SELECT " + attributeName + " FROM " + oldTableNameI
                self.cursor.execute(select)
                resultSelect = self.cursor.fetchall()
                if dataToAdd is None:
                    dataToAdd = resultSelect
                else:
                    for i in range(len(resultSelect)):
                        dataToAdd[i] += resultSelect[i]

            # on ajoute l'attribut a la nouvelle table
            attributeType = self.metadataOfAttribute(oldTableNameI, attributeName)[2]
            s += attributeName + " " + attributeType + ", "

        s = s[:-2] + ")"
        self.cursor.execute(s)

        # on ajoute les donnees a la nouvelle table
        if dataToAdd is not None:
            placeholders = ', '.join(['?'] * len(attribute))
            insert = "INSERT INTO " + tableName + " VALUES (" + placeholders + ")"
            self.cursor.executemany(insert, dataToAdd)
            self.db.commit()

    def renameTable(self, oldTableName, newTableName):
        """
        Renomme une table
        Param:
            oldTableName: le nom de l'ancienne table
            newTableName: le nouveau nom de la table
        """
        s = "ALTER TABLE " + oldTableName + " RENAME TO " + newTableName
        self.cursor.execute(s)
        self.db.commit()

    def addDF(self, table, lhs, rhs):
        """
        Ajoute une DF a la table FuncDep
        Param:
            table: la table concernee
            lhs: le membre de gauche de la DF
            rhs: le membre de droite de la DF
        """
        self.cursor.execute("""INSERT INTO FuncDep VALUES (?, ?, ?)""", (table, lhs, rhs))
        self.db.commit()

    def deleteDF(self, table, lhs, rhs):
        """
        Supprime une DF de la table FuncDep
        Param:
            table: la table concernee
            lhs: le membre de gauche de la DF
            rhs: le membre de droite de la DF
        """
        self.cursor.execute("""DELETE FROM FuncDep WHERE FuncDep.'table' == ? AND lhs == ? AND rhs == ?""",
                            (table, lhs, rhs))
        self.db.commit()

    def deleteTable(self, table):
        """
        Supprime une table de la base de donnees
        Param:
            table: la table a supprimer
        """
        self.cursor.execute("""DROP TABLE ?""", (table,))
        self.db.commit()
