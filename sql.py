import psycopg2
import configparser



config = configparser.ConfigParser()
config.read("settings.ini")
try:
    user = config['DB']['user']
    password = config['DB']['password']
except KeyError:
    raise ValueError ('user или password отсутствуют в файле settings.ini')

def creating_structure(conn):
    with conn.cursor() as cur:
       # создание таблиц
        cur.execute("""
                CREATE TABLE IF NOT EXISTS client(
                    id SERIAL PRIMARY KEY,
                    name VARCHAR(40) NOT NULL,
                    surname VARCHAR(40) NOT NULL,
                    email VARCHAR (40) NOT NULL UNIQUE);
                """)

        cur.execute("""
                CREATE TABLE IF NOT EXISTS phone(
                    id SERIAL PRIMARY KEY,
                    number VARCHAR (11) NOT NULL UNIQUE  
                );
                """)

        cur.execute("""
                    CREATE TABLE IF NOT exists client_phone(
                    client_id INTEGER REFERENCES client(id),
                    phone_id INTEGER REFERENCES phone(id),
                    CONSTRAINT pk PRIMARY KEY (client_id, phone_id)
                    );
                    """)

        return


# Функция, позволяющая добавить нового клиента.
def insert_client (conn, name, surname,email ):
    with conn.cursor() as cur:
        cur.execute("""
                INSERT INTO client (name, surname, email)  VALUES(%s, %s, %s) RETURNING id;
                """, (name, surname, email))
        return print( f'Клиент с номером id: {cur.fetchone()[0]} добавлен в базу данных')  # запрос данных автоматически зафиксирует изменения


def get_client(conn):
    with conn.cursor() as cur:
        cur.execute("""
                SELECT * FROM client;
                """)
        return print(cur.fetchall())

# Функция, позволяющая добавить телефон для существующего клиента.

def insert_phone(conn, client_id, phone):
    with conn.cursor() as cur:
        cur.execute("SELECT id FROM phone WHERE number = %s;", (phone,))
        phone_id = cur.fetchone()

        if not phone_id:
            cur.execute("INSERT INTO phone (number) VALUES (%s) RETURNING id;", (phone,))
            phone_id = cur.fetchone()[0]
        else:
            phone_id = phone_id[0]

        cur.execute("INSERT INTO client_phone VALUES (%s, %s) ON CONFLICT DO NOTHING;", (client_id, phone_id))
        print(f'Клиенту с id:{client_id} добавлен номер {phone}')

# Функция, позволяющая изменить данные о клиенте.

def update_client(conn, client_id, update_name=None, update_surname=None, update_email=None) -> str:
    updates = []
    params = []

    if update_name is not None:
        updates.append("name = %s")
        params.append(update_name)
    if update_surname is not None:
        updates.append("surname = %s")
        params.append(update_surname)
    if update_email is not None:
        updates.append("email = %s")
        params.append(update_email)

    if not updates:
        return "Нет данных для обновления."

    query = f"UPDATE client SET {', '.join(updates)} WHERE id = %s;"
    params.append(client_id)

    with conn.cursor() as cur:
        cur.execute(query, params)

    return f'Данные клиента c id {client_id} изменены'

# Функция, позволяющая удалить телефон для существующего клиента
def delete_phone(conn, client_id, phone) :
    with conn.cursor() as cur:
        cur.execute("""
                SELECT phone_id FROM client_phone cp LEFT JOIN phone p ON cp.phone_id = p.id
                WHERE client_id=%s AND number = %s;
                """, (client_id, phone))
        phone_id = cur.fetchone()[0]
        cur.execute("""
                DELETE FROM client_phone WHERE client_id=%s AND phone_id = %s ;
        """, (client_id, phone_id))

        cur.execute("""
            DELETE FROM phone WHERE id=%s;
            """, (phone_id,))
        return print(f'Телефон {phone} клиента c id {client_id} удален')

# Функция, позволяющая удалить существующего клиента
def delete_client(conn, client_id) -> str:
    with conn.cursor() as cur:
        cur.execute("""
                 DELETE FROM client_phone WHERE client_id=%s;
        """, (client_id,))

        cur.execute("""
                DELETE FROM client WHERE id=%s;
        """, (client_id,))

        return print(f'Клиент c id {client_id} удален')

# Функция, позволяющая найти клиента по его данным: имени, фамилии, email или телефону

def find_client(conn, name=None, surname=None, email=None, phone=None):
    filters = []
    params = []

    if name:
        filters.append("name = %s")
        params.append(name)
    if surname:
        filters.append("surname = %s")
        params.append(surname)
    if email:
        filters.append("email = %s")
        params.append(email)
    if phone:
        filters.append('p."number" = %s')
        params.append(phone)

    if not filters:
        return "Не заданы параметры поиска."

    query = f"""
        SELECT c.name, c.surname, c.email, p."number"
        FROM client c
        LEFT JOIN client_phone cp ON c.id = cp.client_id
        LEFT JOIN phone p ON cp.phone_id = p.id
        WHERE {' AND '.join(filters)};
    """

    with conn.cursor() as cur:
        cur.execute(query, params)
        result = cur.fetchall()

    return result if result else "Клиент не найден"


if __name__ == "__main__":
    with psycopg2.connect(database = 'client_db', user = user, password = password) as conn:
        # creating_structure(conn)
        # insert_client(conn, 'Ivan', 'Ivanov', 'iva20@gmail.com')
        # get_client(conn)
        # insert_phone(conn, 8, '79147512273')
        # update_client(conn, 8, 'Nick', 'Ivanov', 'iva202@gmail.com')
        # delete_phone(conn, 5, '79147512269')
        # delete_client(conn, 5)
        # find_client(conn, phone='79147512271')
        find_client(conn, surname='Ivanov')
    conn.close()
