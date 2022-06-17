from rucio.client import Client

def client(config):
    if config.get("declare_to_rucio", True):
        return Client()		#account=config.get("account", "root"))
    else:
        return None
