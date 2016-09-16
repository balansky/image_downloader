import pymysql


class Instance():
    def __init__(self, db_info):
        self.db_info = db_info
        self.connect_to_db()

    def connect_to_db(self):
        try:
            if hasattr(self, 'cursor'):
                self.cursor.close()
            if hasattr(self, 'cnx'):
                self.cnx.close()
        except Exception:
            pass
        self.cnx = pymysql.connect(**self.db_info)
        self.cursor = self.cnx.cursor()

    def __get_schemas_info(self, table):
        table_schema = {}
        sql = "describe {0};"
        self.cursor.execute(sql.format(table))
        table_schema[table] = self.cursor.fetchall()
        return table_schema

    def __serialize_schema_item(self, item, table_schema):
        schema_item = {}
        for sche in table_schema:
            if sche[5] == 'auto_increment': continue
            if sche[4] == 'CURRENT_TIMESTAMP': continue
            if sche[0] in item:
                schema_item[sche[0]] = item[sche[0]]
            else:
                if sche[4]:
                    schema_item[sche[0]] = sche[4]
                else:
                    schema_item[sche[0]] = self.__assign_default_value(sche)
        return schema_item

    def __render_insert_sql(self, schema_item, table, fields=''):
        insert_attrs = '('
        attr_values = '('
        for key in schema_item.keys():
            insert_attrs = insert_attrs + key + ","
            attr_values = attr_values + "%s,"
        insert_sql = "insert into " + table + " " + insert_attrs[:-1] + ")" + " values " + attr_values[0:-1] + ")"
        if fields and isinstance(fields, list):
            update_sql = ' on duplicate key update '
            for field in fields:
                update_sql += field + "=VALUES({0}),".format(field)
            insert_sql += update_sql[:-1]
        insert_sql += ";"
        return insert_sql

    def __assign_default_value(self, sche):
        sche_type = sche[1].split('(')[0]
        if sche_type == 'int' or sche_type == 'tinyint':
            default_value = 0
        elif sche_type == 'decimal':
            default_value = 0.0
        else:
            default_value = ''
        return default_value

    def serialize_to_tuple(self, schema_item):
        tuple_item = ()
        for item in schema_item.values():
            tuple_item += (item,)
        return tuple_item

    def __test_db(self):
        try:
            self.cursor.execute('show tables;')
            self.cursor.fetchall()
        except pymysql.err.OperationalError:
            self.connect_to_db()

    def execute_select_sql(self, sql, dict_cursor=False):
        self.__test_db()
        if dict_cursor:
            self.cursor.close()
            cursor = self.cnx.cursor(pymysql.cursors.DictCursor)
        else:
            cursor = self.cursor
        cursor.execute(sql)
        sel_data = cursor.fetchall()
        cursor.close()
        self.cursor = self.cnx.cursor()
        return sel_data

    def execute_update_sql(self, sql, data):
        self.__test_db()
        self.cursor.execute(sql, data)
        self.cnx.commit()

    def insert_item_by_schema(self, item, tablename, fields=''):
        table_schema = self.__get_schemas_info(tablename)
        schema_item = self.__serialize_schema_item(item, table_schema)
        insert_sql = self.__render_insert_sql(schema_item, tablename, fields)
        tuple_item = self.serialize_to_tuple(schema_item)
        self.cursor.execute(insert_sql, tuple_item)
        self.cnx.commit()

    def insert_many_items_by_schema(self, items, tablename, fields):
        table_schema = self.__get_schemas_info(tablename)[tablename]
        schema_items = [self.__serialize_schema_item(item, table_schema) for item in items]
        insert_sql = self.__render_insert_sql(schema_items[0], tablename, fields)
        tuple_items = []
        for schema_item in schema_items:
            tuple_items.append(self.serialize_to_tuple(schema_item))
        self.cursor.executemany(insert_sql, tuple_items)
        self.cnx.commit()