import yaml

class Config(dict):

	def __init__(self, config_file):
		cfg = yaml.load(open(config_file, "r"), Loader=yaml.SafeLoader)
		dict.__init__(self, cfg.items())
