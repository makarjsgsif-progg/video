import re

def detect_platform(url: str) -> str:
    if "tiktok.com" in url:
        return "tiktok"
    if "instagram.com" in url or "instagr.am" in url:
        return "reels"
    if "youtube.com" in url or "youtu.be" in url:
        return "youtube"
    return "unknown"