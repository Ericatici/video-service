from pydantic_settings import BaseSettings
from dotenv import load_dotenv

load_dotenv()

class Settings(BaseSettings):
    database_url: str
    redis_url: str
    secret_key: str
    webhook_url: str = "http://localhost:3000/webhook"  # Webhook endpoint for notifications
    rabbitmq_url: str = "amqp://guest:guest@rabbitmq:5672/"

    class Config:
        env_file = ".env"

settings = Settings()
