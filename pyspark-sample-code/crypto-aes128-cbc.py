import myspark
from myspark import spark as spark

from pyspark.sql import functions as F
import base64

# pip install pycryptodomex
from Cryptodome.Cipher import AES, PKCS1_OAEP
from Cryptodome.Hash import HMAC, SHA256, MD5
from Cryptodome.Util.Padding import pad, unpad
from Cryptodome import Random


KEY = 'key is password..'.encode('utf-8')
KEY_128 = HMAC.new(KEY, KEY, digestmod=MD5).digest()


@F.udf('string')
def encrypt(plain_text):
    # plain text -> pad -> encrypt -> add iv -> base64 encode -> cipher text

    plain_pad = pad(plain_text.encode('utf-8'), AES.block_size, style='pkcs7')
    
    iv = Random.get_random_bytes(AES.block_size)
    aes = AES.new(KEY_128, AES.MODE_CBC, iv)
    cipher = aes.encrypt(plain_pad)

    cipher_text = base64.b64encode(iv + cipher).decode('utf-8')

    return cipher_text


@F.udf('string')
def decrypt(cipher_text):
    # cipher text -> base64 decode -> extract iv -> decrypt -> unpad -> plain text

    decoded = base64.b64decode(cipher_text)
    iv = decoded[:16]
    cipher = decoded[16:]

    aes = AES.new(KEY_128, AES.MODE_CBC, iv)
    plain_pad = aes.decrypt(cipher)
    plain = unpad(plain_pad, AES.block_size, style='pkcs7')
    
    plain_text = plain.decode('utf-8')
    return plain_text


df = spark.read.csv('./data/fin.csv', header=True)

df = df.withColumn('enc', encrypt(F.col('이름')))
df = df.withColumn('dec', decrypt(F.col('enc')))

df.select('enc', 'dec', '이름').show(10, False)
