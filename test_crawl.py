from models.domain_discovery_model import DomainModel
import os
from pprint import pprint

path = os.path.dirname(os.path.realpath(__file__))
ddtModel = DomainModel(path)

ddtModel.getAvailableDomains()

session = {"domainId":"AVvpgrtONoPtHdckkl3S"}
print ddtModel.startCrawler(session)

