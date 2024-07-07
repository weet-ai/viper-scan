from viper.core.database import OSVDatabase
import pytest

@pytest.fixture(scope = "module")
def database():

    return OSVDatabase()

def test_download(database):

    database.download()

def test_preprocess(database):

    database.preprocess()

