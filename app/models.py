from pydantic import BaseModel


class BookResult(BaseModel):
    title: str
    url: str
    source: str
    type: str = "PDF"
    author: str | None = None
    year: int | None = None
    fileSize: int | None = None
    relevanceScore: float = 0.5


class SavedBook(BaseModel):
    url: str
    title: str
    author: str | None = None
    year: int | None = None
    savedAt: str
    notes: str | None = None
