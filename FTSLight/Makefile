VERSION = 3.1
PRODUCT = FTS-light
BUILD_HOME = $(HOME)/build
BUILD_DIR = $(BUILD_HOME)/$(PRODUCT)
TAR_DIR = /tmp/$(USER)
TAR_FILE = $(TAR_DIR)/$(PRODUCT)_$(VERSION).tar


FILES = \
charts.html	      GUI.py	     index.html      RELEASE_NOTES  template.html  Version.py  \
config.html	      history.html   input_dir.html  log.html	 Mover.py       Scanner.py     tools.py	 \
GraphiteInterface.py  mover.cfg	static	      WebService.py  


all: tarball

tarball: build $(TAR_DIR)
	cd $(BUILD_DIR); tar cf $(TAR_FILE) *
	@echo
	@echo Tar file $(TAR_FILE) is ready
	@echo
	
build: $(BUILD_DIR)
	find . -type f -name \*.pyc -exec rm -f {} \;
	cp -R $(FILES) $(BUILD_DIR)
	echo 'Version="$(VERSION)"' > $(BUILD_DIR)/Version.py
	
clean:
	rm -rf $(TAR_FILE) $(BUILD_DIR)

$(BUILD_DIR):
	mkdir -p $@

$(TAR_DIR):
	mkdir -p $@



