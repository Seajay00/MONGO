import pymongo

# устанавливаем соединение с MongoDB
# MongoDB должна быть запущена на компьютере, 27017 - стандартный порт
db_client = pymongo.MongoClient("mongodb://localhost:27017/")  # MongoClient('localhost', 27017)

# подключаемся к БД pyloungedb, если её нет, то будет создана
current_db = db_client["t2"]  # dictionary style
# current_db = db_client.pyloungedb - attribute style

# получаем колекцию из нашей БД, если её нет, то будет создана
# Коллекция - это группа документов, которая хранится в БД MongoDB (эквалент таблицы в ркляционных базах)
collection = current_db["collect1"]  # current_db.youtubers

# Коллекции и базы данных в MongoDB created lazily - фактически создаются при вставке в них первого документа
# Данные в MongoDB представляются с помощью JSON-style документов
# можно явно указать желаемый айди, добавив ключ - '_id': n

some_data = {
  'title': 'Название канала',
  'url': 'link_example2',
  'subscribers': 11,
  'views': 22
}

ins_result = collection.insert_one(some_data)  # добавляет одну запись в коллекцию collection
print(collection.find())  # id вставленного объекта

data_set2 = [
    {'title': 'АйТиБорода','url': 'www.youtube.com/c/ITBEARD/', 'subscribers': 227000, 'views': 1200024},
    {'title': 'Диджитализируй!', 'url': 'www.youtube.com/channel/UC9MK8SybZcrHR3CUV4NMy2g/', 'subscribers': 62700, 'views': 960245},
    {'title': 'Senior Software Vlogger', 'url': 'www.youtube.com/user/rojkovdima', 'subscribers': 90700, 'views': 2000000}
]

ins_result = collection.insert_many(data_set2)  # добавляет несколько записей в коллекцию collection
print(collection.find())
# collection.bulk_write()

# Запрос найти первый документ у которого количество подписчиков = subs
subs = 100
print(collection.find_one({'subscribers': subs}))  # {} - критерии запроса
print(collection.count_documents({'subscribers': subs}))  # количество документов в коллекции у которых subs подписчиков

# вывести все документы в коллекции
for data in collection.find():
    print(data)

print('Сложные запросы')
# https://docs.mongodb.com/manual/reference/operator/
print('Количество документов у которых подписчиков > 10 000')
print(collection.count_documents({"subscribers": {"$gt": 10000}}))
print('Количество документов у которых подписчиков < 100')
print(collection.count_documents({"subscribers": {"$lt": 100}}))

# https://docs.mongodb.com/manual/reference/operator/aggregation-pipeline/
print('Все ютуберы к которых > 10 000 подписчиков, отсортированные по полю title')
for channel in collection.find({'subscribers': {'$gt': 10000}}).sort('title'):  # sort('title', -1)
    print('Отбор с сортировкой')
    print(channel,'\n')

# { <operator>: [ <argument1>, <argument2> ... ] }, <argument1> - {'':''}
# print('Все имена ютуберов у которых > 100 000 подписчиков И просмотров больше 1 000 000')
for channel in collection.find({'$and': [{'subscribers': {'$gt': 10000}}, {'views': {'$gt': 1000000}}]}):
    print('Отбор по множеству условий')
    print('Канал:',channel['title'], 'Подписчики:',channel['subscribers'],'\n')


print('2 канала, название которых соответствует регулярному выражению "^Py(.*?)"')
for channel in collection.find({'title': {'$regex': '^Py(.*?)'}}).limit(2):  # groupby, orderby, skip и т.д.
    print('Регулярное выражение')
    print(channel['title'], ' ', channel['subscribers'],'\n')

# переменная с запросом: просмотров от 0 до 1 000 000
query = {'views': {'$in': list(range(0, 1000000))}}
for channel in collection.find(query):

    print('Запрос оформлен как отдельная переменная: просмотров от 0 до 1000 000')
    print('канал:',channel['title'], ' ','подписчики:', channel['subscribers'], 'просмотры:', channel['views'],'\n')

print('Обновление данных в бд')
collection.update_one({'title': 'Название'}, {'$set': {'title': 'Название канала'}})
print(collection.find_one({'title': 'Название канала'}),'\n')
# есть ещё find_one_and_delete, find_one_and_replace и т.д.

print('Find and update')

print(collection.find_one_and_update({'title': 'Название канала'}, {'$set': {'subscribers': 1}}),'\n')

print('Все коллекции:')
for channel in collection.find():
    print(channel)

collection.update_many({'subscribers': {'$gt': 90000}}, {'$set': {'views': 3000000}})
print('После всех обновлений. Каналы с более 90 000 подписчиков:')
for channel in collection.find({'subscribers': {'$gt': 90000}}):
    print(channel)

collection.update_many({'title': {'$eq': 'Название канала'}}, {'$set': {'title': 'Название NEW'}})
print('После всех обновлений. Каналы с более 90 000 подписчиков:')
for channel in collection.find({'title': {'$eq': 'Название NEW'}}):
    print(channel)

print('Все коллекции:')
for channel in collection.find():
    print(channel)
    
# удаление
collection.delete_one({'title': {'$regex': '^Senior'}}) # delete_many, find_one_and_delete
print('После удаления:')
for channel in collection.find():
    print(channel)

# создание индексов
collection.create_index('title')  # , unique=True

# удаление коллекции
collection.drop()
# удаление бд
db_client.drop_database('pyloungedb')
