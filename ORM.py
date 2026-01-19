import sqlalchemy
import json
from sqlalchemy.orm import sessionmaker
from models import create_tables,Publisher, Book, Stock, Sale, Shop

DSN = 'postgresql://postgres:Li275713@localhost:5432/netology_db'
engine = sqlalchemy.create_engine(DSN)

create_tables(engine)

Session = sessionmaker(bind=engine)
session = Session()

with open ('tests_data.json','r') as fd:
    data = json.load(fd)
for record in data:
    model = {
        'publisher': Publisher,
        'shop': Shop,
        'book': Book,
        'stock': Stock,
        'sale': Sale,
    }[record.get('model')]
    session.add(model(id=record.get('pk'), **record.get('fields')))
session.commit()
# publisher1 = Publisher(name='Александр Пушкин')
#
# session.add(publisher1)
# session.commit()
#
# print(publisher1)
#
# b1 = Book(title = "Евгений Онегин",publisher = publisher1)
# b2 = Book(title = "Медный всадник",publisher = publisher1)
# session.add_all([b1,b2])
# session.commit()
for c in session.query(Publisher).all():
    print(c)
name_p = input("Введите имя искомого писателя: ")

p = session.query(Publisher).join(Book.publisher).filter(Publisher.name.like(f'{name_p}'))

for s in p.all():
    #print(s.id,s.name)
    for pb in s.book:
        st = session.query(Book).join(Stock.book).filter(Book.title.like(f'{pb.title}'))
        #print(pb.id, pb.title)

        for st1 in st.all():
            for st2 in st1.stock:
                sh = session.query(Shop).join(Stock.shop).filter(Shop.id == int(f'{st2.id_shop}'))
                sl = session.query(Stock).join(Sale.stock).filter(Stock.id == int(f'{st2.id}'))
                #print(st2.count)

                for sh1 in sh.all():
                    #print(sh1.name)
                    for sl1 in sl.all():
                        for sl2 in sl1.sale:
                            #print(sl2.date_sale)
                            print (f'{pb.title} | {sh1.name} | {sl2.price * sl2.count} | {sl2.date_sale}')


session.close()