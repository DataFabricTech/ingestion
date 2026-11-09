#  Copyright 2021 Collate
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.

"""
JKS keystore → 임시 PEM 파일 → ssl.SSLContext 생성 후 임시 디렉터리 삭제.

OpenMetadata ElasticsearchConnection 스키마에 JKS 필드가 없으므로,
connectionArguments 에 내부 전용 키로 전달하고 get_connection() 에서 제거한다.
"""
from __future__ import annotations

import os
import shutil
import ssl
import tempfile
from typing import Any, Dict, Optional, Tuple

from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.serialization import (
    Encoding,
    NoEncryption,
    PrivateFormat,
    load_der_private_key,
)

# connectionArguments.__root__ 에만 사용; Elasticsearch() 로는 전달하지 않음
METADATA_ES_JKS_CONNECTION_KEY = "__metadata_es_jks__"


def _find_private_key_entry(
    private_keys: Dict[str, Any], alias: Optional[str]
) -> Tuple[str, Any]:
    if not private_keys:
        raise ValueError("JKS 키스토어에 PrivateKey 항목이 없습니다.")
    if alias:
        wanted = alias.strip().lower()
        for a, entry in private_keys.items():
            if a.lower() == wanted:
                return a, entry
        raise ValueError(
            f"JKS 에 alias '{alias}' 인 PrivateKey 항목을 찾을 수 없습니다."
        )
    if len(private_keys) > 1:
        raise ValueError(
            "PrivateKey 항목이 여러 개입니다. es_jks_alias(또는 alias)를 지정하세요."
        )
    return next(iter(private_keys.items()))


def build_ssl_context_from_jks(
    keystore_path: str,
    password: str,
    alias: Optional[str] = None,
) -> ssl.SSLContext:
    """
    JKS를 읽어 임시 PEM을 만든 뒤 SSLContext를 구성하고, 임시 디렉터리를 삭제한다.
    load_cert_chain / load_verify_locations 는 호출 시점에 파일을 읽으므로 삭제 후에도 동작한다.
    """
    import jks

    if not os.path.isfile(keystore_path):
        raise FileNotFoundError(f"JKS 파일이 없습니다: {keystore_path}")

    store_pw = password if password is not None else ""
    ks = jks.KeyStore.load(keystore_path, store_pw, try_decrypt_keys=True)

    if not ks.certs and not ks.private_keys:
        raise ValueError(
            "JKS 키스토어에 trusted certificate 또는 private key 항목이 없습니다."
        )

    ca_pem_parts = []
    for _, cert_entry in ks.certs.items():
        ca_pem_parts.append(ssl.DER_cert_to_PEM_cert(cert_entry.cert))

    tmpdir = tempfile.mkdtemp(prefix="om-es-jks-")
    try:
        ca_path = None
        if ca_pem_parts:
            ca_path = os.path.join(tmpdir, "ca_bundle.pem")
            with open(ca_path, "w", encoding="utf-8") as f:
                f.write("".join(ca_pem_parts))

        if ca_path:
            ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
            ctx.minimum_version = ssl.TLSVersion.TLSv1_2
            ctx.load_verify_locations(cafile=ca_path)
        else:
            ctx = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)

        # elastic-transport/urllib3 가 server_hostname 없이 wrap_socket 할 때
        # check_hostname=True 이면 ValueError 가 난다(IP URL 등).
        ctx.check_hostname = False

        if ks.private_keys:
            _, pk_entry = _find_private_key_entry(ks.private_keys, alias)
            if not pk_entry.is_decrypted():
                pk_entry.decrypt(store_pw)
            if not pk_entry.is_decrypted():
                raise ValueError(
                    "Private key 항목을 복호화할 수 없습니다. 키 전용 비밀번호가 "
                    "스토어 비밀번호와 다르면 현재는 지원하지 않습니다."
                )

            chain_path = os.path.join(tmpdir, "chain.pem")
            key_path = os.path.join(tmpdir, "key.pem")
            cert_pems = "".join(
                ssl.DER_cert_to_PEM_cert(t[1]) for t in pk_entry.cert_chain
            )
            with open(chain_path, "w", encoding="utf-8") as f:
                f.write(cert_pems)

            priv = load_der_private_key(
                pk_entry.pkey_pkcs8, password=None, backend=default_backend()
            )
            key_pem = priv.private_bytes(
                Encoding.PEM, PrivateFormat.PKCS8, NoEncryption()
            )
            with open(key_path, "wb") as f:
                f.write(key_pem)

            ctx.load_cert_chain(chain_path, key_path)

        return ctx
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


def pop_jks_spec_from_connection_args(
    conn_args_root: Optional[Dict[str, Any]],
) -> Tuple[Optional[Dict[str, Any]], Dict[str, Any]]:
    """connectionArguments 복사본에서 JKS 스펙을 꺼내고, Elasticsearch 로 넘길 인자만 남긴다."""
    if not conn_args_root:
        return None, {}
    args = dict(conn_args_root)
    spec = args.pop(METADATA_ES_JKS_CONNECTION_KEY, None)
    return spec, args
