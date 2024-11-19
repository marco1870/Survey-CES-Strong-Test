from pydantic_settings import BaseSettings

class Settings(BaseSettings):

    project_id : str
    dataset_name : str
    table_name_feedbacks: str
    table_name_group: str
    table_name_feedbacks_flagged: str
    username_medallia: str
    password_medallia: str
    rule_id_medallia: str

    class Config:
        env_file = ".env"

settings = Settings()