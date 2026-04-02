import asyncio
import yt_dlp
from io import BytesIO


class Downloader:
    def __init__(self):
        self.opts = {
            'format': 'best[height<=720]',
            'quiet': True,
            'no_warnings': True,
            'extract_flat': False,
            'noplaylist': True,
        }

    async def download(self, url: str) -> tuple[BytesIO | None, str | None]:
        def sync_download():
            try:
                with yt_dlp.YoutubeDL(self.opts) as ydl:
                    info = ydl.extract_info(url, download=False)
                    url_to_download = info['url']
                    import requests
                    resp = requests.get(url_to_download, timeout=30)
                    if resp.status_code == 200:
                        return BytesIO(resp.content), None
                    else:
                        return None, "HTTP error"
            except Exception as e:
                return None, str(e)

        return await asyncio.to_thread(sync_download)