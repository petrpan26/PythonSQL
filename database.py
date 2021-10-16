from dbtype import Type

from typing import Tuple

from engine import Engine

class Schema:
    def __init__(self, mapping: list[Tuple[str, Type]]):
        self.mapping: list[Tuple[str, Type]] = mapping

class Table():
    def __init__(self, id: int, schema: Schema):
        self.table_id = id
        self.schema: Schema = schema
        self.column_id: dict[str, int] = {}
        self.pk_id: int = -1 

        col_id = 1

        for i, (col, type) in enumerate(schema.mapping):
            self.column_id[col] = col_id
            col_id += 1
            if type.is_pk:
                self.pk_id = i
        
        if self.pk_id == -1:
            raise KeyError()
        
    
    def insert_table(self, values: list[str], engine: Engine) -> None:
        pk_type = self.schema.mapping[self.pk_id][1]

        pk_encoding = pk_type.encode(values[self.pk_id])

        base_key = '/{}/{}'.format(self.table_id, pk_encoding)

        engine.put(base_key, 'NULL')

        for i, value in enumerate(values):
            if i == self.pk_id:
                continue
            
            col_name: str
            col_type: Type
            col_name, col_type = self.schema.mapping[i]

            col_key: str = base_key + '/{}'.format(self.column_id[col_name])

            col_encoding: str = col_type.encode(value)

            engine.put(col_key, col_encoding)

            


class Database():
    def __init__(self, engine: Engine):
        self.table_map: dict[str, Table] = {}
        self.table_id: dict[str, int] = {}
        self.id: int = 1
        self.engine = engine
    
    def create_table(self, table_name: str, schema: Schema) -> None:
        if table_name in self.table_map:
            raise KeyError
        
        self.table_map[table_name] = Table(id = self.id, schema=schema)

        self.table_id[table_name] = self.id

        self.id += 1

    def insert_to_table(self, table_name: str, values: list[str]) -> None:
        self.table_map[table_name].insert_table(values, self.engine)