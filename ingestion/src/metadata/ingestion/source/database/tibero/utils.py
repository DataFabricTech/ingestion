from sqlalchemy.engine import reflection

@reflection.cache
def get_columns(self, connection, table_name, schema=None, **kw):
    return null