import myspark
from myspark import spark as spark

from pyspark.sql import functions as F

# pip install pycryptodomex
from Cryptodome.PublicKey import RSA
from Cryptodome.Cipher import PKCS1_OAEP
import base64
import os


KEY_ROOT = './rsa'


def generate_keypair(key_length, name, pwd):
    if os.path.isfile(f'{KEY_ROOT}/{name}.prv.pem') and os.path.isfile(f'{KEY_ROOT}/{name}.pub.pem'):
        return

    prv_key = RSA.generate(key_length)

    # private key
    f = open(f'{KEY_ROOT}/{name}.prv.pem', 'wb+')
    f.write(prv_key.export_key(format='PEM', passphrase=pwd, pkcs=8))
    f.close()

    # public key
    f = open(f'{KEY_ROOT}/{name}.pub.pem', 'wb+')
    f.write(prv_key.publickey().export_key(format='PEM'))
    f.close()


@F.udf('string')
def encrypt(key_stream, plain_text):
    key = RSA.import_key(key_stream)
    cipher = PKCS1_OAEP.new(key)
    cipher_byte = cipher.encrypt(plain_text.encode('utf-8'))
    cipher_text = base64.b64encode(cipher_byte).decode('utf-8')
    return cipher_text


@F.udf('string')
def decrypt(key_stream, pwd, cipher_text):
    key = RSA.import_key(key_stream, passphrase=pwd)
    cipher = PKCS1_OAEP.new(key)
    plain_byte = cipher.decrypt(base64.b64decode(cipher_text))
    plain_text = plain_byte.decode('utf-8')
    return plain_text


def make_directory(directory):
    try:
        if not os.path.exists(directory):
            os.makedirs(directory)
    except OSError:
        print(f'Error: Creating directory: {directory}')


def main():
    make_directory(KEY_ROOT)

    generate_keypair(1024, 'test1024', '123qwe123qwe')
    generate_keypair(2048, 'test2048', '123qwe123qwe')

    df = spark.read.csv('./data/fin.csv', header=True)
    
    pub_key_stream = open(f'{KEY_ROOT}/test2048.pub.pem', 'rb').read()
    df = df.withColumn('pub_enc', encrypt(F.lit(pub_key_stream), F.col('이름')))

    prv_key_stream = open(f'{KEY_ROOT}/test2048.prv.pem', 'rb').read()
    df = df.withColumn('pub_dec', decrypt(
        F.lit(prv_key_stream), F.lit('123qwe123qwe'), F.col('pub_enc')))

    df.select('pub_enc', 'pub_dec', '이름').show(10, False)



if __name__ == '__main__':
    main()
