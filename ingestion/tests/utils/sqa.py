# Copyright 2024 Mobigen
# Licensed under the Apache License, Version 2.0 (the "License")
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Notice!
# This software is based on https://open-metadata.org and has been modified accordingly.

"""SQLAlchemy utilities for testing purposes."""

from typing import Sequence

from sqlalchemy import Column, Integer, String, create_engine
from sqlalchemy.orm import Session, declarative_base
from sqlalchemy.orm.decl_api import DeclarativeMeta

Base = declarative_base()


class User(Base):
    __tablename__ = "users"
    id = Column(Integer, primary_key=True)
    name = Column(String(256))
    fullname = Column(String(256))
    nickname = Column(String(256))
    age = Column(Integer)


class SQATestUtils:
    def __init__(self, connection_url: str):
        self.connection_url = connection_url
        self.engine = create_engine(connection_url)
        self.session = Session(self.engine)

    def load_data(self, data: Sequence[DeclarativeMeta]):
        """load data into the database using sqlalchemy ORM

        Args:
            data List[DeclarativeMeta]: list of ORM objects to load
        """
        self.session.add_all(data)
        self.session.commit()

    def load_user_data(self):
        data = [
            User(name="John", fullname="John Doe", nickname="johnny b goode", age=30),  # type: ignore
            User(name="Jane", fullname="Jone Doe", nickname=None, age=31),  # type: ignore
        ] * 20
        self.load_data(data)

    def create_user_table(self):
        User.__table__.create(bind=self.session.get_bind())

    def close(self):
        self.session.close()
        self.engine.dispose()
