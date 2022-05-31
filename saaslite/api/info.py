from pydantic import BaseModel, HttpUrl


class Result(BaseModel):
    __root__: list[dict]


DB_GET = {
    'response_model': Result,
}

CSV_GET = {
    'response_model': Result,
}


class Upload(BaseModel):
    object_key: str
    upload_url: HttpUrl


UPLOAD_POST = {
    'response_model': Upload,
}
