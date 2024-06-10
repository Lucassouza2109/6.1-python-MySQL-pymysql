# PyMySQL - um cliente MySQL feito em Python Puro
# Doc: https://pymysql.readthedocs.io/en/latest/
# Pypy: https://pypi.org/project/pymysql/
# GitHub: https://github.com/PyMySQL/PyMySQL


# ---------------------

import os
from typing import cast # CAST | REALIZA A CONVERSAO DE TIPO.
import dotenv
import pymysql
import pymysql.cursors


TABLE_NAME = 'customers'
CURRENT_CURSOR = pymysql.cursors.DictCursor

dotenv.load_dotenv()
# CARREGANDO DOTENV. 



connection = pymysql.connect(
    # CONECTANDO COM O ARQUIVO: .env-example
    host=os.environ['MYSQL_HOST'],
    user=os.environ['MYSQL_USER'],
    password=os.environ['MYSQL_PASSWORD'],
    database=os.environ['MYSQL_DATABASE'],
    charset='utf8mb4',
    cursorclass=CURRENT_CURSOR,
)

# ---------------------

with connection:
    with connection.cursor() as cursor:
        cursor.execute(  # type: ignore
            # CRIANDO TABELA. (CAMPOS: id / nome / idade)
            f'CREATE TABLE IF NOT EXISTS {TABLE_NAME} ('
            'id INT NOT NULL AUTO_INCREMENT, '
            'nome VARCHAR(50) NOT NULL, '
            'idade INT NOT NULL, '
            'PRIMARY KEY (id)'
            ') '
        )

# ---------------------

        # CUIDADO: ISSO LIMPA A TABELA
        cursor.execute(f'TRUNCATE TABLE {TABLE_NAME}')  # type: ignore
    connection.commit()

# ---------------------

    # COMECO A MANIPULAR DADOS A PARTIR DAQUI.

    with connection.cursor() as cursor:
        sql = (  # FUNCAO.
            '(nome, idade) '
            'VALUES '
            '(%s, %s) ' # SIGNIF.: Vou enviar esse valores depois
        )               # Aumenta o nivel de Seguranca.

        data = ('Luiz', 18) # DADOS.
        result = cursor.execute(sql, data)  # type: ignore
        # RESULT = EXECUTA A FUNCAO | UTILIZANDO OS DADOS.

    connection.commit()

# ---------------------

    # INSERINDO VALORES COM DICIONARIO - FORA DE ORDEM. 

    with connection.cursor() as cursor:
        sql = (
            f'INSERT INTO {TABLE_NAME} '
            '(nome, idade) '
            'VALUES '
            '(%(name)s, %(age)s) '
        )
        data2 = {
            "age": 37,
            "name": "Le",
        }
        result = cursor.execute(sql, data2)  # type: ignore
        
    connection.commit()

# ---------------------

    # INSERINDO +1 VALOR COM DICIONARIO

    with connection.cursor() as cursor:
        sql = (
            f'INSERT INTO {TABLE_NAME} '
            '(nome, idade) '
            'VALUES '
            '(%(name)s, %(age)s) '
        )
        data3 = (
            {"name": "Sah", "age": 33, },
            {"name": "Júlia", "age": 74, },
            {"name": "Rose", "age": 53, },
        )
        result = cursor.executemany(sql, data3)  # type: ignore
        
    connection.commit()

# ---------------------

    # INSERINDO +1 VALOR COM TUPLA

    with connection.cursor() as cursor:
        sql = (
            f'INSERT INTO {TABLE_NAME} '
            '(nome, idade) '
            'VALUES '
            '(%s, %s) '
        )
        data4 = (
            ("Siri", 22, ),
            ("Helena", 15, ),
            ("Luiz", 18, ),
        )
        result = cursor.executemany(sql, data4)  # type: ignore
        print(sql)
        print(data4)
        print(result)
    connection.commit()

# ---------------------

    # LENDO OS VALORES COM SELECT

    with connection.cursor() as cursor:
        # ENTRADA DO USUARIO
        # menor_id = int(input('Digite o menor id: '))
        # maior_id = int(input('Digite o maior id: '))
        menor_id = 2
        maior_id = 4

        # CONSULTA - SQL
        sql = (
            f'SELECT * FROM {TABLE_NAME} '
            'WHERE id BETWEEN %s AND %s  '
            # %s = PLACEHOLDERS (Sinaliza valores que serao substituídos por dados reais - Ajuda a evitar SQL INJECTION)
        )

        cursor.execute(sql, (menor_id, maior_id))  # type: ignore
        # print(cursor.mogrify(sql, (menor_id, maior_id)))  # type: ignore
        data5 = cursor.fetchall()  # type: ignore
        # FETCHALL = PEGA TODOS OS VALORES. 

        # for row in data5: # PARA CADA LINHA EM DATA5.
        #     print(row)

# ---------------------

    # Apagando com DELETE, WHERE e placeholders no PyMySQL

    with connection.cursor() as cursor:
        sql = (
            f'DELETE FROM {TABLE_NAME} '
            'WHERE id = %s'
        )
        cursor.execute(sql, (1,))  # type: ignore
        connection.commit()

        cursor.execute(f'SELECT * FROM {TABLE_NAME} ')  # type: ignore

        # for row in cursor.fetchall():  # type: ignore
        #     print(row)

# ---------------------

    # Editando com UPDATE, WHERE e placeholders no PyMySQL
    
    with connection.cursor() as cursor:
        cursor = cast(CURRENT_CURSOR, cursor)

        sql = ( # QUERO ATUALIZAR = UPDATE, OS 3 CAMPOS ABAIXO.
            f'UPDATE {TABLE_NAME} ' 
            'SET nome=%s, idade=%s '
            'WHERE id=%s'
        )

        cursor.execute(sql, ('Eleonor', 102, 4))
        
        cursor.execute(
            f'SELECT id from {TABLE_NAME} ORDER BY id DESC LIMIT 1'
        )
        lastIdFromSelect = cursor.fetchone()

        resultFromSelect = cursor.execute(f'SELECT * FROM {TABLE_NAME} ')

        data6 = cursor.fetchall()

        for row in data6:
            print(row)
    
        print('resultFromSelect', resultFromSelect)
        print('len(data6)', len(data6))
        print('rowcount', cursor.rowcount)
        print('lastrowid', cursor.lastrowid)
        print('lastrowid na mão', lastIdFromSelect)

        cursor.scroll(0, 'absolute')
        print('rownumber', cursor.rownumber)

    connection.commit() 