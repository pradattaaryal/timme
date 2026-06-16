from fastapi import APIRouter

router = APIRouter(tags=["Health"])


@router.get("/")
def health_check() -> dict[str, str]:
    return {"message": "API is running"}
