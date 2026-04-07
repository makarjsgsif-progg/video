import re
from enum import Enum
from typing import Optional, Dict, Final


class Platform(str, Enum):
    TIKTOK = "tiktok"
    INSTAGRAM = "instagram"
    TWITTER = "twitter"
    REDDIT = "reddit"
    FACEBOOK = "facebook"
    VIMEO = "vimeo"
    TWITCH = "twitch"
    SNAPCHAT = "snapchat"
    LIKEE = "likee"
    TRILLER = "triller"
    MS_STREAM = "microsoftstream"


PLATFORM_PATTERNS: Final[Dict[Platform, re.Pattern]] = {
    Platform.TIKTOK: re.compile(r"tiktok\.com|vm\.tiktok\.com", re.I),
    Platform.INSTAGRAM: re.compile(r"instagram\.com|instagr\.am|reels", re.I),
    Platform.TWITTER: re.compile(r"twitter\.com|x\.com", re.I),
    Platform.REDDIT: re.compile(r"reddit\.com", re.I),
    Platform.FACEBOOK: re.compile(r"facebook\.com|fb\.watch", re.I),
    Platform.VIMEO: re.compile(r"vimeo\.com", re.I),
    Platform.TWITCH: re.compile(r"twitch\.tv|clips\.twitch\.tv", re.I),
    Platform.SNAPCHAT: re.compile(r"snapchat\.com", re.I),
    Platform.LIKEE: re.compile(r"likee\.(video|com)", re.I),
    Platform.TRILLER: re.compile(r"triller\.co", re.I),
    Platform.MS_STREAM: re.compile(r"microsoftstream\.com", re.I),
}


def detect_platform(url: Optional[str]) -> Optional[Platform]:
    if not url or not isinstance(url, str):
        return None

    clean_url = url.strip()

    for platform, pattern in PLATFORM_PATTERNS.items():
        if pattern.search(clean_url):
            return platform

    return None