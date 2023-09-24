from fastapi import HTTPException
from starlette.status import HTTP_404_NOT_FOUND


class FileNotFoundError(HTTPException):
    def __init__(self, filename: str):
        message = f'File {filename} does not exist'
        super().__init__(HTTP_404_NOT_FOUND, message)
