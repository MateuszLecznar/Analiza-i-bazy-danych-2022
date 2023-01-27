from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, Float, Date, ForeignKey, VARCHAR, Text, SmallInteger

db_string = "postgresql://mateusz:mateusz@localhost:5432/lab12"

engine = create_engine(db_string)

Base = declarative_base()


class User(Base):
    __tablename__ = 'users'
    id = Column(Integer, primary_key=True)
    email = Column(VARCHAR(30))

    def __repr__(self):
        return f"<users(id='{self.id}', email={self.email})>"


class Booking(Base):
    __tablename__ = 'bookings'
    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey('users.id'))
    place_id = Column(Integer, ForeignKey('places.id'))
    start_date = Column(Date)
    end_date = Column(Date)
    price_per_night = Column(Float)
    num_nights = Column(Integer)

    def __repr__(self):
        return f"<users(id='{self.id}', user_id={self.user_id}, place_id='{self.place_id}', " \
               f"start_date={self.start_date}, end_date='{self.end_date}', price_per_night={self.price_per_night}), " \
               f"num_nights={self.num_nights}>"


class Review(Base):
    __tablename__ = 'reviews'
    id = Column(Integer, primary_key=True)
    booking_id = Column(Integer, ForeignKey('bookings.id'))
    rating = Column(SmallInteger)
    review_body = Column(Text)

    def __repr__(self):
        return f"<reviews(id='{self.id}', booking_id={self.booking_id}, rating={self.rating}, " \
               f"review_body={self.review_body})>"


class Host(Base):
    __tablename__ = 'hosts'
    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey('users.id'))

    def __repr__(self):
        return f"<hosts(id='{self.id}', user_id={self.user_id})>"


class Place(Base):
    __tablename__ = 'places'
    id = Column(Integer, primary_key=True)
    host_id = Column(Integer, ForeignKey('hosts.id'))
    address = Column(VARCHAR(50))
    city_id = Column(Integer, ForeignKey('cities.id'))

    def __repr__(self):
        return f"<places(id='{self.id}', host_id={self.host_id}, address={self.address}, city_id={self.city_id})>"


class City(Base):
    __tablename__ = 'cities'
    id = Column(Integer, primary_key=True)
    name = Column(VARCHAR(30))
    country_id = Column(Integer, ForeignKey('countries.id'))

    def __repr__(self):
        return f"<cities(id='{self.id}', name={self.name}, country_id={self.country_id})>"


class Country(Base):
    __tablename__ = 'countries'
    id = Column(Integer, primary_key=True)
    country_code = Column(VARCHAR(10))
    name = Column(VARCHAR(50))

    def __repr__(self):
        return f"<countries(id='{self.id}', country_code={self.country_code}, name={self.name})>"


Base.metadata.create_all(engine)
