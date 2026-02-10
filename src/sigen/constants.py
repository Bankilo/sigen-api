"""Constants for Sigenergy cloud API."""

REGION_BASE_URLS = {
    "eu": "https://api-eu.sigencloud.com/",
    "cn": "https://api-cn.sigencloud.com/",
    "apac": "https://api-apac.sigencloud.com/",
    "us": "https://api-us.sigencloud.com/",
}

PASSWORD_AES_KEY = "sigensigensigenp"
PASSWORD_AES_IV = "sigensigensigenp"

OAUTH_CLIENT_ID = "sigen"
OAUTH_CLIENT_SECRET = "sigen"

# Northbound API base URLs (same regional pattern)
NB_API_BASE_URLS = {
    "eu": "https://api-eu.sigencloud.com/",
    "cn": "https://api-cn.sigencloud.com/",
    "apac": "https://api-apac.sigencloud.com/",
    "us": "https://api-us.sigencloud.com/",
}


class NBMode:
    """Northbound operational mode integer values."""
    MSC = 0   # Maximum Self-Consumption
    FFG = 5   # Fully Feed-in to Grid
    VPP = 6   # VPP
    NBI = 8   # North Bound (required for northbound instructions)
