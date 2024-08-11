from viper.core.database import VulnerabilitiesDatabase
from dotenv import load_dotenv
import os

load_dotenv()

def test_download():

    url = os.getenv("DATASET_URL")
    vuln = VulnerabilitiesDatabase(dataset_url = url)
    vuln.download()