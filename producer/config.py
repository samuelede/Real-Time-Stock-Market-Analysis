import logging
import os
from dotenv import load_dotenv # type: ignore

load_dotenv()

logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# FIX 1: Set the host correctly
BASEURL = "alpha-vantage.p.rapidapi.com"
url = f"https://{BASEURL}/query"

# FIX 2: Ensure headers use the clean host string
headers = {
    "x-rapidapi-key": os.getenv("RAPIDAPI_KEY"),
    "x-rapidapi-host": BASEURL # Use the variable directly
}
