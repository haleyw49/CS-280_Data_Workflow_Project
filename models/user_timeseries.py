from sqlalchemy.orm import declarative_base
from sqlalchemy import Column, Integer, String, Date, Float

Base = declarative_base()

class User_Timeseries(Base):
    __tablename__ = "user_timeseries"
    id = Column(Integer, primary_key=True, not_null=True)
    user_id = Column(String, not_null=True)
    followers_count = Column(Integer, not_null=True)
    following_count = Column(Integer, not_null=True)
    tweet_count = Column(Integer, not_null=True)
    listed_count = Column(Integer, not_null=True)
    date = Column(Date, not_null=True)

    def __repr__(self) -> str:
        return f"User_Timeseries(id={self.id}, user_id={self.user_id}, followers_count={self.followers_count}, following_count={self.following_count}, tweet_count={self.tweet_count}, listed_count={self.listed_count}, date={self.date})"

