from sqlalchemy.engine import reflection
from sqlalchemy.sql import sqltypes, text

from sqlalchemy.util import warn

@reflection.cache
def get_table_names(self, connection, schema=None, **kw):
    schema = self.denormalize_name(schema or self.default_schema_name)

    # note that table_names() isn't loading DBLINKed or synonym'ed tables
    if schema is None:
        schema = self.default_schema_name

    query = "SELECT T.TABLE_NAME 'TABLE_NAME' FROM SYSTEM_.SYS_TABLES_ T, SYSTEM_.SYS_USERS_ U WHERE "
    # 임시로, 모든 스키마의 테이블 목록을 반환해보자.. (다른스키마 만들어 데이터 넣기 귀찮...)
    # if self.exclude_schemaspaces:
    #     query += "U.USER_NAME NOT IN (%s) AND " % (", ".join(["'%s'" % ts for ts in self.exclude_schemaspaces]))
    query += (
        "T.TABLE_TYPE = 'T' "
        "AND T.USER_ID = U.USER_ID "
        "AND U.USER_NAME = :schema_name "
        "ORDER BY T.TABLE_NAME;"
    )

    cursor = connection.execute(text(query), dict(schema_name=schema))

    return [self.normalize_name(row[0]) for row in cursor]


@reflection.cache
def get_columns(self, connection, table_name, schema=None, **kw):
    schema = self.denormalize_name(schema or self.default_schema_name)

    if schema is None:
        schema = self.default_schema_name

    # 스키마 제한 일단 없애자...
    query = """
        SELECT U.USER_NAME USER_NAME
        , T.TABLE_NAME TABLE_NAME
        , C.COLUMN_NAME COLUMN_NAME
        , DECODE(C.DATA_TYPE, 1, 'CHAR', 12, 'VARCHAR', -8, 'NCHAR', -9, 'NVARCHAR', 2, 'DECIMAL', 6, 'FLOAT', 8, 'DOUBLE', 7, 'REAL', -5, 'BIGINT', 4, 'INTEGER', 5, 'SMALLINT', 9, 'DATE', 30, 'BLOB', 40, 'CLOB', 20001, 'BYTE', 20002, 'NIBBLE', -7, 'BIT', -100, 'VARBIT', 10003, 'GEOMETRY') DATA_TYPE
        , CASE WHEN (C.DATA_TYPE != 2 OR C.DATA_TYPE != 6) THEN C.PRECISION END AS CHAR_LENGTH_COL
        , DECODE(C.IS_NULLABLE, 'F', 'N', 'T', 'Y') NULLABLE
        , C.DEFAULT_VAL DATA_DEFAULT
        , COM.COMMENTS COMMENTS
    FROM SYSTEM_.SYS_USERS_ U
        INNER JOIN SYSTEM_.SYS_TABLES_ T ON U.USER_ID = T.USER_ID
        INNER JOIN SYSTEM_.SYS_COLUMNS_ C ON T.TABLE_ID = C.TABLE_ID
        LEFT OUTER JOIN SYSTEM_.SYS_COMMENTS_ COM ON T.TABLE_NAME = COM.TABLE_NAME AND C.COLUMN_NAME = COM.COLUMN_NAME AND U.USER_NAME = COM.USER_NAME
    WHERE 1=1 
--    AND U.USER_NAME NOT IN ('PUBLIC', 'SYSTEM_', 'SYS')
    AND U.USER_NAME NOT IN ('PUBLIC', 'SYSTEM_')
    AND U.USER_NAME = :schema_name
    AND T.TABLE_NAME = :table_name
    ORDER BY U.USER_NAME, T.TABLE_NAME, C.COLUMN_ORDER ;
    """

    results = connection.execute(
        text(query),
        dict(
            table_name=self.denormalize_name(table_name),
            schema_name=self.denormalize_name(schema),
        ),
    )

    columns = []

    for user_name, table_name, column_name, data_type, char_length_col, nullable, data_default, comments in results:
        if data_type in ("CHAR", "VARCHAR", "NCHAR", "NVARCHAR"):
            data_type = self.ischema_names.get(data_type)(char_length_col)
        else:
            try:
                data_type = self.ischema_names[data_type]
            except KeyError:
                warn("Did not recognize type '%s' of column '%s'" % (data_type, column_name))
                data_type = sqltypes.NULLTYPE


        # cdict = {
        #     "name": colname,
        #     "type": coltype,
        #     "nullable": nullable,
        #     "default": default,
        #     "autoincrement": "auto",
        #     "comment": row.comments,
        #     "system_data_type": raw_coltype,
        # }

        column_dict = {
            "name": column_name,
            "type": data_type,
            "nullable": nullable == "Y",
            "default": data_default,
            "autoincrement": "auto",
            "comment": comments,
        }
        columns.append(column_dict)

    return columns