import hashlib
import aiofiles
import os

def generate_task_id(user_id: int, url: str) -> str:
    return hashlib.md5(f"{user_id}{url}".encode()).hexdigest()

async def save_temp_file(data: bytes, suffix: str = ".mp4") -> str:
    filename = f"temp_{hashlib.md5(data).hexdigest()}{suffix}"
    path = os.path.join("/tmp", filename)
    async with aiofiles.open(path, "wb") as f:
        await f.write(data)
    return path

async def remove_temp_file(path: str):
    if os.path.exists(path):
        os.remove(path)