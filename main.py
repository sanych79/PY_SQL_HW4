import psycopg2
import sys
import configparser

config = configparser.ConfigParser()
config.read("settings.ini")

database_mane = config["database"]["name"]
user_name = config["database"]["user"]
user_password = config["database"]["password"]

with psycopg2.connect(database="postgres", user="postgres", password="101010") as conn:
    with conn.cursor() as cur:

        def create_db_structure():
            """Функция создания структуры БД. Схемы, таблиц, ограничений"""
            # создание схемы person
            cur.execute("""
                        CREATE SCHEMA IF NOT EXISTS person;
                        """)
            # удаление таблиц
            cur.execute("""
                        DROP TABLE  IF EXISTS person.person_info;
                        """)
             # создание таблицы person_info
            cur.execute("""
                        CREATE TABLE IF NOT EXISTS person.person_info(
                        person_id SERIAL PRIMARY KEY,
                        person_first_name VARCHAR(100) NOT NULL,
                        person_second_name VARCHAR(100) NOT NULL);
                        """)
            # создание таблицы email
            cur.execute("""
                        CREATE TABLE IF NOT EXISTS person.email(
                        email_name VARCHAR(150) NOT NULL UNIQUE,
                        person_id INT REFERENCES person.person_info(person_id),
                        CONSTRAINT pk PRIMARY KEY (email_name, person_id));
                        """)
            # создание таблицы phone_number
            cur.execute("""
                        CREATE TABLE IF NOT EXISTS person.phone_number(
                        phone_number VARCHAR(150) NOT NULL UNIQUE,
                        person_id INT REFERENCES person.person_info(person_id),
                        CONSTRAINT pk1 PRIMARY KEY(phone_number, person_id));
                        """)

        # Блок проверок
        def check_email_unique(email):
            """Проверка уникальности email"""
            cur.execute("""
                        SELECT person_id FROM person.email WHERE email_name=%s ;
                        """, (email,))
            return len(cur.fetchall())

        def check_phone_number_unique(phone_number):
            """Проверка уникальности phone_number"""
            cur.execute("""
                        SELECT person_id FROM person.phone_number WHERE phone_number=%s ;
                        """, (phone_number,))
            return len(cur.fetchall())

        def check_person(person_id, first_name, second_name):
            """Функция проверки существрования клиента  first_name, second_name с идентификатором person_id"""
            try:
                cur.execute("""
                            SELECT person_id FROM person.person_info WHERE person_first_name=%s 
                                and person_second_name=%s and person_id=%s;
                            """, (first_name, second_name, person_id))
                cur.fetchall()[0]
            except Exception:
                print(f'В БД нет клиента - {first_name} {second_name} c идентификатором {person_id}')
                return 0
            else:
                return 1

        def check_phone_number(person_id, phone_number):
            """Функция проверки существрования номера телефонв у клиента с идентификатором person_id"""
            try:
                cur.execute("""
                            SELECT person_id FROM person.phone_number WHERE phone_number=%s 
                                and person_id=%s;
                            """, (phone_number, person_id))
                cur.fetchall()[0]
            except Exception:
                return 0
            else:
                return 1

        def check_email(person_id, email):
            """Функция проверки существрования email у клиента с идентификатором person_id"""
            try:
                cur.execute("""
                            SELECT person_id FROM person.email WHERE email_name=%s 
                                and person_id=%s;
                            """, (email, person_id))
                cur.fetchall()[0]
            except Exception:
                return 0
            else:
                return 1

        # Блок основных функций
        def insert_new_person(first_name, second_name, phone_number, email):
            """Функция добавляющая нового клиента"""
            if check_phone_number_unique(phone_number) != 0:
                sys.exit(f'Номер телефона {phone_number} не уникален!!!')
            if check_email_unique(email) != 0:
                sys.exit(f'e-mail: {email} не уникален!!!')

            cur.execute("""
                        INSERT INTO person.person_info(person_first_name, person_second_name) VALUES(%s, %s);
                        """, (first_name, second_name))
            cur.execute("""select person_id from person.person_info order by person_id desc""")
            person_id_ = cur.fetchone()[0]
            cur.execute("""
                        INSERT INTO person.email(email_name, person_id) VALUES(%s, %s);
                        """, (email, person_id_))
            cur.execute("""
                        INSERT INTO person.phone_number(phone_number, person_id) VALUES(%s, %s);
                        """, (phone_number, person_id_))
            print(f'Создан новый Клиент {first_name} {second_name}, телефон {phone_number}, email {email}')

        def insert_phone_number(person_id, first_name, second_name, phone_number):
            """Функция добавления телефона для существующего клиента"""
            if check_phone_number_unique(phone_number) != 0:
                sys.exit(f'Номер телефона {phone_number} не уникален!!!')

            if check_person(person_id, first_name, second_name) == 1:
                try:
                    cur.execute("""
                                INSERT INTO person.phone_number(phone_number, person_id) VALUES(%s, %s);
                                """, (phone_number, person_id))
                except Exception:
                    print(f'У клиент {first_name} {second_name} уже есть телефон {phone_number}')
                else:
                    print(f'Клиенту {first_name} {second_name} добавлен телефон {phone_number}')

        def insert_email(person_id, first_name, second_name, email):
            """Функция добавления email для существующего клиента"""
            if check_email_unique(email) != 0:
                sys.exit(f'e-mail: {email} не уникален!!!')

            if check_person(person_id, first_name, second_name) == 1:
                try:
                    cur.execute("""
                                INSERT INTO person.email(email_name, person_id) VALUES(%s, %s);
                                """, (email, person_id))
                except Exception:
                    print(f'У клиент {first_name} {second_name} уже есть адрес {email}')
                else:
                    print(f'Клиенту {first_name} {second_name} добавлен адрес {email}')

        def update_phone_number(person_id, first_name, second_name, phone_number_old, phone_number_new):
            """Функция изменения существующего номера телефона"""
            if check_phone_number_unique(phone_number_new) != 0:
                sys.exit(f'Номер телефона {phone_number_new} не уникален!!!')

            if check_person(person_id, first_name, second_name) == 1:
                if check_phone_number(person_id, phone_number_old):
                    cur.execute("""
                                UPDATE person.phone_number SET phone_number=%s WHERE person_id=%s and phone_number=%s;
                                """, (phone_number_new, person_id, phone_number_old))
                    print(f'У клиента - {first_name} {second_name} изменен телефона {phone_number_old} '
                          f'на {phone_number_new}')
                else:
                    print(f'У клиента - {first_name} {second_name} нет телефон {phone_number_old}')

        def update_email(person_id, first_name, second_name, email_old, email_new):
            """Функция изменения существующего номера телефона"""
            if check_email_unique(email_new) != 0:
                sys.exit(f'e-mail: {email_new} не уникален!!!')

            if check_person(person_id, first_name, second_name) == 1:
                if check_email(person_id, email_old):
                    cur.execute("""
                                UPDATE person.email SET email_name=%s WHERE person_id=%s and email_name=%s;
                                """, (email_new, person_id, email_old))
                    print(f'У клиента - {first_name} {second_name} изменен e-mail {email_old} '
                          f'на {email_new}')
                else:
                    print(f'У клиента - {first_name} {second_name} нет e-mail {email_old}')

        def rename_person(person_id, first_name, second_name, first_name_new, second_name_new):
            """Функция Переименования клиента"""
            if check_person(person_id, first_name, second_name) == 1:
                    cur.execute("""
                                UPDATE person.person_info SET person_first_name=%s, person_second_name=%s
                                    WHERE person_id=%s;
                                """, (first_name_new, second_name_new, person_id))
                    print(f'Клиент - {first_name} {second_name} переименован в {first_name_new} '
                          f'на {second_name_new}')

        def delete_phone_number(person_id, first_name, second_name, phone_number):
            """5.1 Функция, позволяющая удалить телефон для существующего клиента"""
            chk = input(f'Вы уверены, что хотите удалить у клиена {first_name} {second_name} '
                        f'номер {phone_number}? (Y/N)')
            if chk == 'Y':
                if check_person(person_id, first_name, second_name) == 1:
                    if check_phone_number(person_id, phone_number):
                        cur.execute("""
                                DELETE FROM person.phone_number 
                                    WHERE person_id=%s and phone_number =%s;
                                """, (person_id, phone_number))
                        print(f'У клиента - {first_name} {second_name} удален телефон {phone_number}')
                    else:
                        print(f'У клиента - {first_name} {second_name} нет телефона {phone_number}')

        def delete_email(person_id, first_name, second_name, email):
            """5.2 Функция, позволяющая удалить email для существующего email"""
            chk = input(f'Вы уверены, что хотите удалить у клиента {first_name} {second_name} email {email}? (Y/N)')
            if chk == 'Y':
                if check_person(person_id, first_name, second_name) == 1:
                    if check_email(person_id, email):
                        cur.execute("""
                                DELETE FROM person.email 
                                    WHERE person_id=%s and email_name =%s;
                                    """, (person_id, email))
                        print(f'У клиента - {first_name} {second_name} удален email {email}')
                    else:
                        print(f'У клиента - {first_name} {second_name} нет email {email}')

        def delete_person(person_id, first_name, second_name):
            """6. Функция, позволяющая удалить существующего клиента"""
            chk = input(f'Вы уверены, что хотите удалить клиента {first_name} {second_name}? (Y/N)')
            if chk == 'Y':
                if check_person(person_id, first_name, second_name) == 1:
                    cur.execute("""
                                DELETE FROM person.email 
                                    WHERE person_id=%s;
                            """, (person_id,))
                    cur.execute("""
                                DELETE FROM person.phone_number 
                                    WHERE person_id=%s;
                            """, (person_id,))
                    cur.execute("""
                                DELETE FROM person.person_info 
                                    WHERE person_id=%s;
                            """, (person_id,))
                    print(f'Клиент - {first_name} {second_name} удален')

        def serch_person(first_name, second_name, email, phone_number):
            """Функция  поиска клиента по имени, фамилии, почте или телефону"""
            cur.execute("""
                        select pi2.person_id, pi2.person_first_name, pi2.person_second_name,
                            e.email_name, pn.phone_number 
                            from person.person_info pi2
                            left join person.email e on e.person_id = pi2.person_id
                            left join person.phone_number pn on pn.person_id = pi2.person_id
                        where pi2.person_first_name = %s and pi2.person_second_name = %s and 
                              (e.email_name = %s or pn.phone_number = %s);
                        """, (first_name, second_name, email, phone_number))
            res = cur.fetchall()
            if len(res) != 0:
                print(res)
            else:
                print(f'Клиент {first_name} {second_name} c email {email} или телефоном {phone_number} не найден!')

        per = 0

        while per != 12:
            print('1. Создать структуру БД (таблицы)')
            print('2. Добавить нового клиент')
            print('3. Добавить телефон для существующего клиента')
            print('4. Добавить email для существующего клиента')
            print('5. Изменить телефон у существующего клиента')
            print('6. Изменить email у существующего клиента')
            print('7. Переименовать клиента')
            print('8. Удалить номер телефона у существующего клиента')
            print('9. Удалить email у существующего клиента')
            print('10. Удалить клиента')
            print('11. Поиск клиента по Имени, Фамилии, email или номеру телефона')
            print('12. Выход')
            per = int(input('Введите значение меню>>>'))

            if per == 1:
                print('***Создать структуру БД (таблицы)***')
                create_db_structure()
            elif per == 2:
                print('***Добавить нового клиент***')
                first_name = str(input('Введите Имя>>>'))
                second_name = str(input('Введите Фамилию>>>'))
                phone_number = str(input('Введите номер телефона>>>'))
                email = str(input('Введите email>>>'))
                insert_new_person(first_name, second_name, phone_number, email)
            elif per == 3:
                print('***Добавить телефон для существующего клиента***')
                first_name = str(input('Введите Имя>>>'))
                second_name = str(input('Введите Фамилию>>>'))
                person_id = int(input('Введите идентификатор  клиента>>>'))
                phone_number = str(input('Введите номер телефона>>>'))
                insert_phone_number(person_id, first_name, second_name, phone_number)
            elif per == 4:
                print('***Добавить email для существующего клиента***')
                first_name = str(input('Введите Имя>>>'))
                second_name = str(input('Введите Фамилию>>>'))
                person_id = int(input('Введите идентификатор  клиента'))
                email = str(input('Введите email>>>'))
                insert_email(person_id, first_name, second_name, email)
            elif per == 5:
                print('***Изменить телефон у существующего клиента***')
                first_name = str(input('Введите Имя>>>'))
                second_name = str(input('Введите Фамилию>>>'))
                person_id = int(input('Введите идентификатор  клиента>>>'))
                phone_number_old = str(input('Введите старый номер телефона>>>'))
                phone_number_new = str(input('Введите новый номер телефона>>>'))
                update_phone_number(person_id, first_name, second_name, phone_number_old, phone_number_new)
            elif per == 6:
                print('***Изменить email у существующего клиента***')
                first_name = str(input('Введите Имя>>>'))
                second_name = str(input('Введите Фамилию>>>'))
                person_id = int(input('Введите идентификатор  клиента>>>'))
                email_old = str(input('Введите старый email>>>'))
                email_new = str(input('Введите новый email>>>'))
                update_email(person_id, first_name, second_name, email_old, email_new)
            elif per == 7:
                print('***Переименовать клиента***')
                first_name = str(input('Введите Имя>>>'))
                second_name = str(input('Введите Фамилию>>>'))
                person_id = int(input('Введите идентификатор  клиента>>>'))
                first_name_new = str(input('Введите новое имя>>>'))
                second_name_new = str(input('Введите новую фамилию>>>'))
                rename_person(person_id, first_name, second_name, first_name_new, second_name_new)
            elif per == 8:
                print('***Удалить номер телефона у существующего клиента***')
                first_name = str(input('Введите Имя>>>'))
                second_name = str(input('Введите Фамилию>>>'))
                person_id = int(input('Введите идентификатор  клиента>>>'))
                phone_number = str(input('Введите номер телефона>>>'))
                delete_phone_number(person_id, first_name, second_name, phone_number)
            elif per == 9:
                print('***Удалить email у существующего клиента***')
                first_name = str(input('Введите Имя>>>'))
                second_name = str(input('Введите Фамилию>>>'))
                person_id = int(input('Введите идентификатор  клиента>>>'))
                email = str(input('Введите email>>>'))
                delete_email(person_id, first_name, second_name, email)
            elif per == 10:
                print('***Удалить клиента***')
                first_name = str(input('Введите Имя>>>'))
                second_name = str(input('Введите Фамилию>>>'))
                person_id = int(input('Введите идентификатор  клиента>>>'))
                delete_person(person_id, first_name, second_name)
            elif per == 11:
                print('***Поиск клиента по Имени, Фамилии, email или номеру телефона***')
                first_name = str(input('Введите Имя>>>'))
                second_name = str(input('Введите Фамилию>>>'))
                phone_number = str(input('Введите номер телефона>>>'))
                email = str(input('Введите email>>>'))
                serch_person(first_name, second_name, email, phone_number)
            elif per == 12:
                print('Конец работы программы! Выход!')
            else:
                print(f'Пункт меню {per} отсутсвует! Повторите свой выбор!')
    conn.commit()