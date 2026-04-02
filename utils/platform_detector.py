import re

def detect_platform(url: str) -> str | None:
    patterns = {
        "tiktok": r"(tiktok\.com|vm\.tiktok\.com)",
        "instagram": r"(instagram\.com|instagr\.am|reels)",
        "youtube": r"(youtube\.com|youtu\.be|shorts)",
        "twitter": r"(twitter\.com|x\.com)",
        "reddit": r"(reddit\.com)",
        "facebook": r"(facebook\.com|fb\.watch)",
        "vimeo": r"(vimeo\.com)",
        "twitch": r"(twitch\.tv|clips\.twitch\.tv)",
        "pinterest": r"(pinterest\.com)",
        "snapchat": r"(snapchat\.com)",
        "likee": r"(likee\.video|likee\.com)",
        "triller": r"(triller\.co)",
        "microsoftstream": r"(microsoftstream\.com)"
    }
    for platform, pattern in patterns.items():
        if re.search(pattern, url, re.IGNORECASE):
            return platform
    return None