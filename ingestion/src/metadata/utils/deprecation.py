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

"""
Announce method deprecation
"""
import warnings
from functools import wraps


def deprecated(message: str, release: str):
    """Decorator factory to accept specific messages for each function"""

    def _deprecated(fn):
        @wraps(fn)
        def inner(*args, **kwargs):
            warnings.simplefilter("always", DeprecationWarning)
            warnings.warn(
                f"[{fn.__name__}] will be deprecated in the release [{release}]: {message}",
                category=DeprecationWarning,
            )
            warnings.simplefilter("default", DeprecationWarning)

            return fn(*args, **kwargs)

        return inner

    return _deprecated
