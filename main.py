import json

import sqlalchemy
from sqlalchemy.orm import sessionmaker

from connect_bd import key
from models import create_tables, Stock, Shop, Book, Publisher, Sale

DSN = key
engine = sqlalchemy.create_engine(DSN)

create_tables(engine)

Session = sessionmaker(bind=engine)
session = Session()

# Загрузка данных
def load_data_from_json(session, json_path='fixtures/tests_data.json'):
    with open(json_path, encoding='utf-8') as f:
        data = json.load(f)
    # Добавляем данные
    for record in data:
        model = {
            'stock': Stock,
            'shop': Shop,
            'book': Book,
            'publisher': Publisher,
            'sale': Sale
        }[record.get('model')] # Для получения модели из словаря
        session.add(model(id=record.get('pk'), **record.get('fields'))) # Добавляем запись
    
    session.commit()

# Запрос к БД
def fetch_publisher_sales(publisher_info):
    # Создание сессии
    session = Session()

    # Определяем, является ли ввод числом (ID)
    try:
        publisher_id = int(publisher_info)
        publisher_query = session.query(Publisher).filter(Publisher.id == publisher_id)
    except ValueError:
        publisher_query = session.query(Publisher).filter(Publisher.name == publisher_info)

    # Получаем издателя
    publisher = publisher_query.one_or_none()

    if publisher:
        # Запрос к БД для получения фактов покупки книг определенного издателя
        sales = session.query(Book.title, Shop.name, Sale.price, Sale.date_sale).\
            join(Stock, Book.id == Stock.id_book).\
            join(Sale, Stock.id == Sale.id_stock).\
            join(Shop, Stock.id_shop == Shop.id).\
            filter(Book.id_publisher == publisher.id).all()
        
        # Выводим результаты
        for sale in sales:
            print(f'Книга: {sale.title} | Магазин: {sale.name} | Цена: {sale.price} | Дата: {sale.date_sale}')
    else:
        print("Издатель не найден.")

    # Закрываем сессию
    session.close()


if __name__ == '__main__':
    # Загружаем данные
    session = Session()
    load_data_from_json(session)
    session.close()
    # Запрашиваем у пользователя имя или ID издателя
    publisher_input = input("Введите имя или идентификатор издателя: ")
    fetch_publisher_sales(publisher_input)