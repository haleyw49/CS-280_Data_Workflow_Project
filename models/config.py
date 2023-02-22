from airflow.models import Variable
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

def create_dburl():
  hostname = Variable.get("DATABASE_HOSTNAME")
  username = Variable.get("DATABASE_USERNAME")
  password = Variable.get("DATABASE_PASSWORD")
  port = Variable.get("DATABASE_PORT")
  database = Variable.get("DATABASE_NAME")
  return f"postgresql+psycopg2://{username}:{password}@{hostname}:{port}/{database}"

engine = create_engine(create_dburl())
Session = sessionmaker(bind=engine)