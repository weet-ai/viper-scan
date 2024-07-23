from viper.core.database import VulnerabilitiesDatabase

def test_download():

    vuln = VulnerabilitiesDatabase()
    vuln.download_dataset()