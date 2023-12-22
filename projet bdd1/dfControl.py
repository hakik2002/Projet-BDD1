import copy
from DataBaseControl import DataBaseControl

class dfControl:
    """Database Functional Dependency Handler"""

    TABLE = 'table_name'
    LHS = 'lhs'
    RHS = 'rhs'

    def __init__(self, database):
        """
        Constructor for DfHandler

        Parameters:
        - database (str): The name of the database.
        """
        self.dbh = DataBaseControl(database)
        self.dataBaseName = database

    def is_dep(self, table, lhs, rhs):
        """
        Check if the table exists, and lhs and rhs are attributes of the table.

        Parameters:
        - table (str): The name of the table.
        - lhs (str): The left-hand side of the functional dependency.
        - rhs (str): The right-hand side of the functional dependency.

        Returns:
        - bool: True if the functional dependency is valid, False otherwise.
        """
        list_tables = self.get_table_name()
        if table not in list_tables:
            return False

        list_attributes = self.dbh.get_table_attributes(table)

        # Check if the names in lhs are attributes of the table
        s_lhs = lhs.split()

        for item in s_lhs:
            if item not in list_attributes:
                return False

        # Check that rhs contains only one element
        if rhs.count(' ') != 0:
            return False

        # Check if rhs is an attribute of the table
        if rhs not in list_attributes:
            return False

        # Check if attributes of the functional dependency are included in the table attributes
        dep_attributes = set(lhs.split() + [rhs])
        if not dep_attributes.issubset(set(list_attributes)):
            return False

        return True

    def __dep_exist(self, table, lhs, rhs):
        """
        Check if the functional dependency exists.

        Parameters:
        - table (str): The name of the table.
        - lhs (str): The left-hand side of the functional dependency.
        - rhs (str): The right-hand side of the functional dependency.

        Returns:
        - bool: True if the functional dependency exists, False otherwise.
        """
        r = self.dbh.get_dependency(table, lhs, rhs)
        return r is not None and len(r) == 3

    def remove_dep(self, table, lhs, rhs):
        """
        Remove a functional dependency.

        Parameters:
        - table (str): The name of the table.
        - lhs (str): The left-hand side of the functional dependency.
        - rhs (str): The right-hand side of the functional dependency.

        Returns:
        - bool: True if the functional dependency is removed, False otherwise.
        """
        if self.__dep_exist(table, lhs, rhs):
            self.dbh.remove_dependency(table, lhs, rhs)
            return True
        else:
            return False

    def get_all_dep(self):
        """
        Get all functional dependencies.

        Returns:
        - list: A list of all functional dependencies in the database.
        """
        return self.dbh.get_all_dependencies()

    def getCle(self, table):
        """
        Get the keys of the table.

        Parameters:
        - table (str): The name of the table.

        Returns:
        - list: A list of keys for the given table.
        """
        cles, supercle = self.recurse_cle(self.dbh.get_table_attributes(table), [], [], table)
        return cles

    def insert_dep(self, table, lhs, rhs):
        """
        Insert a functional dependency.

        Parameters:
        - table (str): The name of the table.
        - lhs (str): The left-hand side of the functional dependency.
        - rhs (str): The right-hand side of the functional dependency.

        Returns:
        - bool: True if the functional dependency is inserted, False otherwise.
        """
        if self.is_dep(table, lhs, rhs) and not self.__dep_exist(table, lhs, rhs):
            r = self.dbh.insert_dependency(table, lhs, rhs)
            return r is not None
        else:
            return False

    def edit_dep(self, table, lhs, rhs, new_data, what_modif):
        """
        Edit a functional dependency.

        Parameters:
        - table (str): The name of the table.
        - lhs (str): The left-hand side of the functional dependency.
        - rhs (str): The right-hand side of the functional dependency.
        - new_data (str): The new data to be updated.
        - what_modif (str): The type of modification (TABLE, LHS, or RHS).

        Returns:
        - bool: True if the functional dependency is edited, False otherwise.
        """
        if not self.__dep_exist(table, lhs, rhs):
            return False

        if what_modif == dfControl.TABLE:
            if not self.is_dep(new_data, lhs, rhs) or self.__dep_exist(new_data, lhs, rhs):
                return False
            else:
                return self.dbh.edit_table_dependency(table, lhs, rhs, new_data)
        elif what_modif == dfControl.RHS:
            if not self.is_dep(table, lhs, new_data) or self.__dep_exist(table, lhs, new_data):
                return False
            else:
                return self.dbh.edit_rhs_dependency(table, lhs, rhs, new_data)
        elif what_modif == dfControl.LHS:
            if not self.is_dep(table, new_data, rhs) or self.__dep_exist(table, new_data, rhs):
                return False
            else:
                return self.dbh.edit_lhs_dependency(table, lhs, rhs, new_data)
        else:
            return False

    def is_bcnf(self, table_name):
        """
        Check if the table is in Boyce-Codd Normal Form (BCNF).

        Parameters:
        - table_name (str): The name of the table.

        Returns:
        - bool: True if the table is in BCNF, False otherwise.
        """
        all_lhs = self.dbh.get_all_lhs(table_name)
        if all_lhs is None:
            # Handle the case where get_all_lhs returns None
            return False

        all_attributes = self.dbh.get_table_attributes(table_name)

        for lhs in all_lhs:
            lhs_tab = lhs.split()  # Assuming elements in lhs are separated by spaces
            for attribute in all_attributes:
                if attribute not in lhs_tab:
                    if not self.is_logic_consequence(table_name, lhs, attribute, False):
                        return False

        return True

    def get_all_table_in_func_dep(self):
        """
        Get all table names present in the FuncDep table.

        Returns:
        - list: A list of table names present in the FuncDep table.
        """
        return self.dbh.get_table_names()

    def get_table_name(self):
        """
        Get all table names present in the database.

        Returns:
        - list: A list of all table names present in the database.
        """
        return self.dbh.get_table_names()

    def get_dep_by_relation(self, table):
        """
        Get all dependencies concerning a table.

        Parameters:
        - table (str): The name of the table.

        Returns:
        - list: A list of all dependencies concerning the specified table.
        """
        return self.dbh.get_dependency_by_relation(table)

    def is_3nf(self, table):
        """
        Check if the table is in Third Normal Form (3NF).

        Parameters:
        - table (str): The name of the table.

        Returns:
        - bool: True if the table is in 3NF, False otherwise.
        """
        if self.prem_3nf(table) or self.lhs_3nf(table):
            return True
        else:
            return False

    def prem_3nf(self, table):
        """
        Check if all attributes are in at least one key.

        Parameters:
        - table (str): The name of the table.

        Returns:
        - bool: True if all attributes are in at least one key, False otherwise.
        """
        attribute = self.dbh.get_table_attributes(table)
        cles = self.get_cle(table)
        for att in attribute:
            is_in_cle = False
            for cle in cles:
                if att in cle:
                    is_in_cle = True
            if not is_in_cle:
                return False
        return True

    def lhs_3nf(self, table):
        """
        Check if all Lhs are keys.

        Parameters:
        - table (str): The name of the table.

        Returns:
        - bool: True if all Lhs are keys, False otherwise.
        """
        tab_lhs = self.dbh.get_all_lhs(table)
        tab_cle = self.get_cle(table)
        for i in range(0, len(tab_lhs)):
            lhs = tab_lhs[i]

            if lhs.split() not in tab_cle:
                return False
        return True

    def __get_attribute_never_in_rhs(self, table):
        """
        Get attributes of the table not present in the Rhs of associated functional dependencies.

        Parameters:
        - table (str): The name of the table.

        Returns:
        - list: A list of attributes not present in the Rhs of associated functional dependencies.
        """
        attribute = self.dbh.get_table_attributes(table)
        allRhs = self.dbh.get_all_rhs(table)
        never = []
        for item in attribute:
            if item not in allRhs:
                never.append(item)
        return never

    def __can_continue(self, ligne, nbr_attribute):
        """
        Check if we can continue.

        Parameters:
        - ligne (list): A list of attributes.
        - nbr_attribute (int): The number of attributes.

        Returns:
        - bool: True if we can continue, False otherwise.
        """
        if len(ligne) == 0:
            return True
        for cle in ligne:
            if len(cle) >= nbr_attribute:
                return False
        return True

    def __exept(self, A, B):
        """
        Return A\\B.

        Parameters:
        - A (list): List A.
        - B (list): List B.

        Returns:
        - list: A list containing elements in A but not in B.
        """
        retour = []
        for item in A:
            if item not in B:
                retour.append(item)
        return retour

    def is_a_key(self, future_key, attribute, table):
        """
        Check if it's a key.

        Parameters:
        - future_key (list): The list of attributes to check if it's a key.
        - attribute (list): The list of all attributes.
        - table (str): The name of the table.

        Returns:
        - bool: True if it's a key, False otherwise.
        """
        att = copy.deepcopy(attribute)
        possible_key = copy.deepcopy(future_key)
        result = self.__do_fermeture(self.dbh.get_dependency_by_relation(table), possible_key)
        result.sort()
        att.sort()
        return result == att

    def sans_bac_n(self, s):
        """
        Return the name without newline.

        Parameters:
        - s (str): A string.

        Returns:
        - str: The string without newline characters.
        """
        s2 = copy.deepcopy(s)
        if '\n' in s2:
            s2.remove('\n')
        return s2

    def can_add_to_cle(self, cles, item):
        """
        Check if the key can be added.

        Parameters:
        - cles (list): A list of keys.
        - item (list): The key to check.

        Returns:
        - bool: True if the key can be added, False otherwise.
        """
        for cle in cles:
            if self.__is_in(cle, item):
                return False
        return True

    def recurse_cle(self, attribute, cles, supercle, table):
        """
    Recursively find the closure of attributes and identify keys and superkeys.

    Parameters:
    - attribute: List of attributes of the table
    - cles: Current list of identified keys
    - supercle: Current list of identified superkeys
    - table: Name of the table

    Returns:
    Tuple containing the final lists of keys (cles) and superkeys (supercle)
    """
            # Check if recursion can continue based on the current state of keys and superkeys
        if (not self.__can_continue(cles, len(attribute))) or (not self.__can_continue(supercle, len(attribute))):
            return cles, supercle
            # Initial case when no keys or superkeys are identified yet 
        if len(cles)==0 and len(supercle)==0:
            newLine=self.__get_attribute_never_in_rhs(table)
            newLine.sort()
            if len(newLine)==0 :
                for i in range(0, len(attribute)):
                    it=[attribute[i]]
                    it.sort()
                    if self.is_a_key(it,attribute, table) and self.can_add_to_cle(cles,[it]):
                        if it not in supercle:
                            cles.append(it)
                    if newLine not in supercle:
                        supercle.append(it)
            else:
                if self.is_a_key(newLine,attribute, table) and self.can_add_to_cle(cles, [newLine]) and newLine not in cles:
                    cles.append(newLine)
                if newLine not in supercle:
                    supercle.append(newLine)
            return self.recurse_cle(attribute, cles, supercle, table)
        else:
            newSuperCle=[]
            for item in supercle:
                rajout=self.__exept(attribute, item)
                for item_to_add in rajout:
                    new=[item_to_add]
                    new.extend(item)
                    new.sort()
                    if self.is_a_key(new,attribute, table) and self.can_add_to_cle(cles, new) and new not in cles:
                        cles.append(new)
                    if new not in newSuperCle:
                        newSuperCle.append(new)
                if self.is_a_key(item, attribute, table) and item not in newSuperCle:
                    newSuperCle.append(item)
            return self.recurse_cle(attribute, cles, newSuperCle, table)

    def get_cle(self, table):
        """
    Get the keys of the table.

    Parameters:
    - table: Name of the table

    Returns:
    List of keys of the table
    """
        cles = self.recurse_cle(self.dbh.get_table_attributes(table), [], [], table)
        return cles

    def getSuperCle(self, table):
        """
    Get the superkeys of the table.

    Parameters:
    - table: Name of the table

    Returns:
    List of superkeys of the table
    """
        cles, supercle=self.recurse_cle(self.dbh.get_table_attributes(table), [], [], table)
        return supercle

    def __is_in(self, small, big):
        """
    Check if one list is included in another.

    Parameters:
    - small: List to check for inclusion
    - big: List in which inclusion is checked

    Returns:
    True if 'small' is in 'big', else False
    """
        for sItem in small:
            sItemIsInBig=False
            for bItem in big:
                if sItem==bItem:
                    sItemIsInBig=True
            if sItemIsInBig==False:
                return False
        return True

    def get_couverture_minimale(self, table):
        """
        Get the minimal coverage ('irreducible set of functional dependencies') of the table.
        """
        dep_tab = self.dbh.get_dependency_by_relation(table)
        cles = self.get_cle(table)
        res = []

        for i in range(0, len(dep_tab)):
            item = dep_tab[i]
            lhs = item[0]
            rhs = item[1]
            if lhs.split() not in cles:
                result = self.__do_fermeture(dep_tab, lhs.split())
                if not self.__is_in(result, rhs.split()):
                    res.append(item)
        return res

    def __do_fermeture(self, dep_tab, item):
        """Get the closure of an item with respect to the set of functional dependencies.

    Parameters:
    - dep_tab: List of functional dependencies
    - item: List of attributes for which closure is to be calculated

    Returns:
    List representing the closure of 'item'"""
        result = copy.deepcopy(item)
        for i in range(0, len(dep_tab)):
            new_item = dep_tab[i]
            lhs = new_item[0]
            rhs = new_item[1]

            if self.__is_in(result, lhs.split()):
                for item_to_add in rhs.split():
                    if item_to_add not in result:
                        result.append(item_to_add)
        result.sort()
        return result


    def is_logic_consequence(self, table, lhs, item, modif):
        """
    Check if an item is a logical consequence.

    Parameters:
    - table: Name of the table
    - lhs: Left-hand side of the functional dependency
    - item: Item to check for logical consequence
    - modif: Boolean indicating whether 'item' is being added to 'lhs'

    Returns:
    True if 'item' is a logical consequence of 'lhs', else False
    """
        all_lhs = self.dbh.get_all_lhs(table)
        dep_tab = self.dbh.get_dependency_by_relation(table)
        lhs_tab = lhs.split()

        if modif:
            lhs_tab.append(item)
            result = self.__do_fermeture(dep_tab, lhs_tab)
        else:
            result = self.__do_fermeture(dep_tab, lhs_tab)

        item_tab = item.split()

        return self.__is_in(result, item_tab)
