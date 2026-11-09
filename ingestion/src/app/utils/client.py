from urllib import parse

def postgresql_url(host: str, user: str, passwd: str, db: str):
    return 'postgresql+psycopg2://%s:%s@%s/%s' % (user, parse.quote(passwd), host, db)

def data_to_dict(result: dict, delNRToValue=True, escape: set=set()) -> list:
    columns = result['columns']
    datas = result['data']
    newList = []
    for record in datas:
        dic = {}
        for fieldIdx in range(len(record)):
            # 이스케이프 처리
            if escape and escape.intersection({columns[fieldIdx]}) and record[fieldIdx] and len(record[fieldIdx]) == 1:
                dic[columns[fieldIdx]] = format(ord(record[fieldIdx]), '#04x')
            elif isinstance(record[fieldIdx], str):
                if delNRToValue:
                    dic[columns[fieldIdx]] = record[fieldIdx].replace('\r', '').replace('\n', '')
                else:
                    dic[columns[fieldIdx]] = record[fieldIdx]
            else:
                dic[columns[fieldIdx]] = record[fieldIdx]
        newList.append(dic)
    return newList

import sqlmodel
from logging import Logger
from sqlalchemy.sql.elements import ColumnElement
from sqlalchemy import func
from typing import List


class SqlalchemyOrmClient:
    def __init__(self, url, charset: str, sql_log=False, pool_size=5, max_overflow=10):
        self._charset = charset
        self._url = url
        self._sql_log = sql_log
        self._engine = sqlmodel.create_engine(self._url, encoding=charset, echo=self._sql_log, pool_size=pool_size, max_overflow=max_overflow)

    def __enter__(self):
        try:
            self._session = self.__get_session()
        except Exception as e:
            raise e
        
    def __get_session(self):
        return sqlmodel.Session(self._engine)
    
    def commit(self):
        if self._session:
            self._session.commit()

    def rollback(self):
        if self._session:
            self._session.rollback()

    # 모든 테이블 생성
    def create_all(self):
        sqlmodel.SQLModel.metadata.create_all(self._engine)

    # SQL 구문 실행
    def execute(self, sql, param=None):
        return self._session.execute(sql, param)
    

    def select_sql(self, sql, param=None):
        results = self.execute(sql, param)
        return {'columns': list(results.keys()), 'data': results.fetchall()}
    
    def select_sql_res(self, sql, param=None):
        results = self.execute(sql, param)
        return {'columns': list(results.keys()), 'data': results}
    
    def select_sql_dict(self, sql, param=None):
        return data_to_dict(self.select_sql(sql, param))
    
    def select_one_dict(self, sql, param=None):
        r: list = data_to_dict(self.select_sql(sql, param))
        if r:
            return r[0]
        else:
            return {}

    # Object Select Query 생성
    def create_select_statement(self, model_class, options=None, where=None, group_by=None, order_by=None):
        statement = sqlmodel.select(model_class)
        if isinstance(options, ColumnElement):
            statement = statement.options(options)
        if isinstance(where, ColumnElement):
            statement = statement.where(where)
        if isinstance(group_by, ColumnElement):
            statement = statement.group_by(group_by)
        if isinstance(order_by, ColumnElement):
            statement = statement.order_by(order_by)
        return statement
    
    # 한건 fetch
    def select_one(self, statement):
        return self._session.exec(statement).first()

    # 모든건 fetch
    def select_all(self, statement, offset=None, limit=None):
        s = statement
        if offset:
            s = s.offset(offset)
        if limit:
            s = s.limit(limit)
        return self._session.exec(s).all()
    
    # INSERT
    def insert(self, obj):
        self._session.add(obj)

    def insert_commit(self, obj):
        self.insert(obj)
        self.commit()
        self.refresh(obj)

    def refresh(self, obj):
        self._session.refresh(obj)

    def __exit__(self, exc_type, exc_val, exc_tb):
        try:
            self._session.close()
        except:
            print('raise exception during exit.')
    

    # Category count 쿼리 작성
    def get_json_count_list(self, model_class,
                            json_key=None,
                            where=None,
                            json_contains_target=None,
                            group_by=None,
                            order_by=None,
                            value_list: list = None):
        statement = sqlmodel.select(*[func.count().filter((json_contains_target.contains(f"""[{{"{json_key}": "{value}"}}]""")).label(value)) for value in value_list])

        if isinstance(where, ColumnElement):
            statement = statement.where(where)
        if isinstance(group_by, ColumnElement):
            statement = statement.group_by(group_by)
        if isinstance(order_by, ColumnElement):
            statement = statement.order_by(order_by)
        return statement
    
    # Object Select Query 생성
    def create_select_specific_statement(self, model_class, options=None, where=None, group_by=None, order_by=None):
        statement = sqlmodel.select(*model_class)
        if isinstance(options, ColumnElement):
            statement = statement.options(options)
        if isinstance(where, ColumnElement):
            statement = statement.where(where)
        if isinstance(group_by, ColumnElement):
            statement = statement.group_by(group_by)
        if isinstance(order_by, ColumnElement):
            statement = statement.order_by(order_by)
        return statement
    
    # Sub Query 생성
    def create_subquery_statement(self, model_class, options=None, where=None, group_by=None, order_by=None):
        statement = sqlmodel.select(*model_class)
        if isinstance(options, ColumnElement):
            statement = statement.options(options)
        if isinstance(where, ColumnElement):
            statement = statement.where(where)
        if isinstance(group_by, ColumnElement):
            statement = statement.group_by(group_by)
        if isinstance(order_by, ColumnElement):
            statement = statement.order_by(order_by)
        return statement.subquery()
    
    # Object Update Query 생성
    def create_update_statement(self, model_class, where=None, values=None, order_by=None):
        statement = sqlmodel.update(*model_class)
        if isinstance(where, ColumnElement):
            statement = statement.where(where)
        if isinstance(values, ColumnElement):
            statement = statement.values(values)
        if isinstance(order_by, ColumnElement):
            statement = statement.order_by(order_by)
        return statement
