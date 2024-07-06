from viper.core.database import OpenSourceVulnerabilities

def test_download():

    vuln = OpenSourceVulnerabilities()
    vuln.download_dataset()