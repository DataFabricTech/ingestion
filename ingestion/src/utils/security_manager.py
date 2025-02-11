import os
import base64
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from hashlib import sha256
from utils.process_config import config


class SecurityManager:

    cryptkey = config.crypt_key
    cryptbytekey = sha256(cryptkey.encode()).digest()

    IV_LENGTH = 12
    TAG_LENGTH = 16

    @staticmethod
    def encodeWithcryptkey(data: str):
        iv = os.urandom(SecurityManager.IV_LENGTH)
        data_bytes = bytearray(data, 'utf-8')
        cipher = Cipher(algorithms.AES(SecurityManager.cryptbytekey), modes.GCM(iv))

        encryptor = cipher.encryptor()
        enc_data = encryptor.update(data_bytes) + encryptor.finalize()
        result = iv + encryptor.tag + enc_data

        return bytes(base64.b64encode(result)).decode(encoding='utf-8')

    @staticmethod
    def decodeWithcryptkey(b64_data: str) -> str:
        enc_data = base64.b64decode(b64_data)

        iv = enc_data[:SecurityManager.IV_LENGTH]
        tag = enc_data[SecurityManager.IV_LENGTH:(SecurityManager.IV_LENGTH+SecurityManager.TAG_LENGTH)]
        ciphertext = enc_data[(SecurityManager.IV_LENGTH+SecurityManager.TAG_LENGTH):]

        cipher = Cipher(algorithms.AES(SecurityManager.cryptbytekey), modes.GCM(iv, tag))
        decryptor = cipher.decryptor()
        decoded_data = decryptor.update(ciphertext) + decryptor.finalize()

        return bytes(decoded_data).decode(encoding='utf-8')


