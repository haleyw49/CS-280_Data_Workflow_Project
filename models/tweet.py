from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Date, Float

Base = declarative_base()

class Tweet(Base):
    __tablename__ = "tweet"
    id = Column(Integer, primary_key=True, not_null=True)
    tweet_id = Column(String, not_null=True)
    user_id = Column(String, not_null=True)
    text = Column(String, not_null=True)
    created_at = Column(Date, not_null=True)

    def __repr__(self) -> str:
        return f"Tweet(id={self.id}, tweet_id={self.tweet_id}, user_id={self.user_id}, text={self.text}, created_at={self.created_at})"

