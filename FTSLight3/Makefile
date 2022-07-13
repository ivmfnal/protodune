VERSION = 3.8.0
PRODUCT = FTS-light3
BUILD_HOME = $(HOME)/build
BUILD_DIR = $(BUILD_HOME)/$(PRODUCT)
TAR_DIR = /tmp/$(USER)
TAR_FILE = $(TAR_DIR)/$(PRODUCT)_$(VERSION).tar


FILES = \
	Version.py  Mover.py Scanner.py tools.py historydb.py \
	GraphiteInterface.py mover.cfg uid.py \
	fts3client logs \
	WebService.py GUI.py static \
	charts.html index.html template.html config.html history.html input_dir.html log.html	 


all: tarball

tarball: build $(TAR_DIR)
	cd $(BUILD_DIR); tar cf $(TAR_FILE) *
	@echo
	@echo Tar file $(TAR_FILE) is ready
	@echo
	
build: $(BUILD_DIR)
	find . -type f -name \*.pyc -exec rm -f {} \;
	cp -R $(FILES) $(BUILD_DIR)
	cp -R run $(BUILD_DIR)
	echo 'Version="$(VERSION)"' > $(BUILD_DIR)/Version.py
	
clean:
	rm -rf $(TAR_FILE) $(BUILD_DIR)

$(BUILD_DIR):
	mkdir -p $@

$(TAR_DIR):
	mkdir -p $@



