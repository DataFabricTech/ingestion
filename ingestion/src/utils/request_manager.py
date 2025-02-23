import requests
from requests.auth import HTTPBasicAuth


class RequestManager:

    def request_post(self, url: str, data: str):
        return requests.post(url=url, data=data)

    def request_put(self, url: str, data=None):
        return requests.put(url=url, data=data)

    def request_get(self, url: str, params=None):
        return requests.get(url=url, params=params)

    def request_get_with_auth(self, url: str, user: str, password: str, params=None):
        return requests.get(url=url, auth=HTTPBasicAuth(user, password), params=params)

    def request_patch_with_auth(self, url: str, user: str, password: str, params=dict):
        return requests.patch(url=url, auth=HTTPBasicAuth(user, password), json=params)

    def request_del(self, url: str, user: str, password: str):
        return requests.delete(url=url, auth=HTTPBasicAuth(user, password))