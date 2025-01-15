# -*- coding: UTF-8 -*-
# @author: zhulw
# @file: crypto
# @time: 2021-02-03
# @desc: 常见加密算法
import base64
import hashlib
import hmac
from binascii import a2b_hex
from binascii import b2a_hex

from Crypto.Cipher import AES
from Crypto.Cipher import DES
from Crypto.Cipher import DES3
from Crypto.Cipher import PKCS1_v1_5
from Crypto.PublicKey import RSA
from Crypto.Util.Padding import pad, unpad


def myhash(data, flag="md5"):
    """
    hash 函数 默认 md5 可以使用 sha1 等
    :param data: 待加密的文本
    :param flag: md5/sha1/sha256等
    :return:
    """
    if hasattr(hashlib, flag):
        my_hash = getattr(hashlib, flag)()
    else:
        raise Exception(f"未定义hash类型: {flag}")
    if isinstance(data, str):
        data = data.encode()
    my_hash.update(data)
    hash_data = my_hash.hexdigest()
    return hash_data


def aesEncrypt(content, key, iv="", encode="BASE64", decode=None):
    """
    AES 加密
    :param content: 待加密文本
    :param key: AES key
    :param iv: AES iv(CBC模式才需要)
    :param encode: HEX/BASE64
    :param decode: key/iv 解码方式 None代表 .encode() "HEX" a2b_hex "BASE64"
    :return:
    """
    if decode == "HEX":
        key = a2b_hex(key)
        if iv:
            iv = a2b_hex(iv)
    elif decode == "BASE64":
        key = base64.b64decode(key)
        if iv:
            iv = base64.b64decode(iv)
    else:
        key = key.encode()
        if iv:
            iv = iv.encode()
    mode = "CBC" if iv else "ECB"
    if mode == "ECB":
        aes = AES.new(key, AES.MODE_ECB)
    else:
        aes = AES.new(key, AES.MODE_CBC, iv)
    encrypt_bytes = aes.encrypt(pad(content.encode(), AES.block_size))
    if encode == "BASE64":
        result = base64.b64encode(encrypt_bytes)
    else:
        result = b2a_hex(encrypt_bytes)
    return result.decode()


def aesDecrypt(content, key, iv="", encode="BASE64", decode=None):
    """
    AES 解密
    :param content: 待解密文本
    :param key: AES key
    :param iv: AES iv(CBC模式才需要)
    :param encode: BASE64/HEX
    :param decode: key/iv 解码方式 None代表 .encode() "HEX" a2b_hex "BASE64"
    :return:
    """
    if decode == "HEX":
        key = a2b_hex(key)
        if iv:
            iv = a2b_hex(iv)
    elif decode == "BASE64":
        key = base64.b64decode(key)
        if iv:
            iv = base64.b64decode(iv)
    else:
        key = key.encode()
        if iv:
            iv = iv.encode()
    if encode == "BASE64":
        encrypt_bytes = base64.b64decode(content)
    else:
        encrypt_bytes = a2b_hex(content)
    mode = "CBC" if iv else "ECB"
    if mode == "ECB":
        aes = AES.new(key, AES.MODE_ECB)
    else:
        aes = AES.new(key, AES.MODE_CBC, iv)
    result = unpad(aes.decrypt(encrypt_bytes), AES.block_size)
    return result.decode()


def rsaEncryptByKey(content, key, encode="BASE64"):
    """

    :param content: 待加密文本
    :param key: PublicKey
    :param encode: BASE64/HEX
    :return:
    """
    if "KEY" in key:
        key = key
    else:
        key = "-----BEGIN PUBLIC KEY-----\n" + key + "\n-----END PUBLIC KEY-----"
    rsakey = RSA.importKey(key)
    rsa = PKCS1_v1_5.new(rsakey)
    if encode == "BASE64":
        result = base64.b64encode(rsa.encrypt(content.encode()))
    else:
        result = b2a_hex(rsa.encrypt(content.encode()))
    return result.decode()


def rsaEncryptByModule(content, module, pubKey="10001", encode="BASE64"):
    """

    :param content: 待加密文本
    :param module: HEX 编码的 module
    :param pubKey: 默认 10001
    :param encode: BASE64/HEX
    :return:
    """
    pubKey = int(pubKey, 16)
    modules = int(module, 16)
    pubobj = RSA.construct((modules, pubKey), False)
    public_key = pubobj.publickey().exportKey().decode()
    return rsaEncryptByKey(content, public_key, encode)


def rsaDecryptByKey(content, key, encode="BASE64"):
    """

    :param content: 待解密文本
    :param key: PrivateKey
    :param encode: BASE64/HEX
    :return:
    """
    key = "-----BEGIN PRIVATE KEY-----\n" + key + "\n-----END PRIVATE KEY-----"
    rsakey = RSA.importKey(key)
    rsa = PKCS1_v1_5.new(rsakey)
    if encode == "BASE64":
        result = rsa.decrypt(base64.b64decode(content), b"")
    else:
        result = rsa.decrypt(a2b_hex(content), b"")
    return result.decode()


def desEncrypt(content, key, iv="", encode="BASE64", decode=None, is_des3=False):
    """
    DES 加密
    :param content: 待加密文本
    :param key: DES key
    :param iv: DES iv(CBC模式才需要)
    :param encode: HEX/BASE64
    :param decode: key/iv 解码方式 None代表 .encode() "HEX" a2b_hex "BASE64"
    :param is_des3: 是否为 3DES
    :return:
    """
    if decode == "HEX":
        key = a2b_hex(key)
        if iv:
            iv = a2b_hex(iv)
    elif decode == "BASE64":
        key = base64.b64decode(key)
        if iv:
            iv = base64.b64decode(iv)
    else:
        key = key.encode()
        if iv:
            iv = iv.encode()
    mode = "CBC" if iv else "ECB"
    if mode == "ECB":
        if is_des3:
            des = DES3.new(key, DES3.MODE_ECB)
        else:
            des = DES.new(key=key, mode=DES.MODE_ECB)
    else:
        if is_des3:
            des = DES3.new(key, DES3.MODE_CBC, iv)
        else:
            des = DES.new(key=key, mode=DES.MODE_CBC, iv=iv)
    encrypt_bytes = des.encrypt(pad(content.encode(), DES.block_size))
    if encode == "BASE64":
        result = base64.b64encode(encrypt_bytes)
    else:
        result = b2a_hex(encrypt_bytes)
    return result.decode()


def desDecrypt(content, key, iv="", encode="BASE64", decode=None, is_des3=False):
    """
    DES 解密
    :param content: 待解密文本
    :param key: DES key
    :param iv: DES iv(CBC模式才需要)
    :param encode: HEX/BASE64
    :param decode: key/iv 解码方式 None代表 .encode() "HEX" a2b_hex "BASE64"
    :param is_des3: 是否为 3DES
    :return:
    """
    if decode == "HEX":
        key = a2b_hex(key)
        if iv:
            iv = a2b_hex(iv)
    elif decode == "BASE64":
        key = base64.b64decode(key)
        if iv:
            iv = base64.b64decode(iv)
    else:
        key = key.encode()
        if iv:
            iv = iv.encode()
    mode = "CBC" if iv else "ECB"
    if encode == "BASE64":
        encrypt_bytes = base64.b64decode(content)
    else:
        encrypt_bytes = a2b_hex(content)
    if mode == "ECB":
        if is_des3:
            des = DES3.new(key, DES3.MODE_ECB)
        else:
            des = DES.new(key=key, mode=DES.MODE_ECB)
    else:
        if is_des3:
            des = DES3.new(key, DES3.MODE_CBC, iv)
        else:
            des = DES.new(key=key, mode=DES.MODE_CBC, iv=iv)
    result = unpad(des.decrypt(encrypt_bytes), DES.block_size)
    return result.decode()


def hmac_hash(content, key, method="sha1", encode="BASE64", decode=None):
    """
    hmac hash
    :param content: 待加密文本
    :param key: hmac key
    :param method: 哈希方法 sha1/sha256/sha512
    :param encode: HEX/BASE64
    :param decode: key/iv 解码方式 None代表 .encode() "HEX" a2b_hex "BASE64"
    :return:
    """
    if hasattr(hashlib, method):
        my_hash = getattr(hashlib, method)
    else:
        raise Exception(f"未定义hash类型: {method}")
    if decode == "HEX":
        key = a2b_hex(key)
    elif decode == "BASE64":
        key = base64.b64decode(key)
    else:
        key = key.encode()
    hmac_obj = hmac.new(key, content.encode(), my_hash)
    if encode == "BASE64":
        result = base64.b64encode(hmac_obj.digest())
    else:
        result = b2a_hex(hmac_obj.digest())
    return result.decode()
