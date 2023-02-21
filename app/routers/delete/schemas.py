from enum import Enum
from pydantic import BaseModel, PositiveInt

class Env(Enum):
    dev = "dev"
    prod = "prod"

class BaseConfig(BaseModel):
    class Config:
        anystr_strip_whitespace = True
        use_enum_values = True

class PushMessageData(BaseConfig):
    from_id: PositiveInt
    env: str
