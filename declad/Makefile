PRODUCT = declad
VERSION = 1.8.0
BUILD_HOME = $(HOME)/build
BUILD_DIR = $(BUILD_HOME)/$(PRODUCT)
TEMPLATES_DIR = $(BUILD_DIR)/templates
TAR_DIR = /tmp/$(USER)
TAR_FILE = $(TAR_DIR)/$(PRODUCT)_$(VERSION).tar


FILES = config.py logs.py mover.py samweb_client.py tools.py web_server.py \
	declad.py historydb.py metacat_client.py rucio_client.py scanner.py xrootd_scanner.py graphite_interface.py

# - not needed as we are moving EOS to EOS using xrootd fts3client.py context.py request.py


all: tarball

tarball: build $(TAR_DIR)
	cd $(BUILD_DIR); tar cf $(TAR_FILE) *
	@echo
	@echo Tarfile $(TAR_FILE) created
	@echo

build: $(BUILD_DIR) $(TEMPLATES_DIR)
	@echo
	@echo building version $(VERSION) ...
	@echo
	find . -type f -name \*.pyc -exec rm -f {} \;
	echo 'Version="$(VERSION)"' > $(BUILD_DIR)/version.py
	cp $(FILES) $(BUILD_DIR)
	cp templates/*.html $(TEMPLATES_DIR)

clean:
	rm -rf $(TAR_FILE) $(BUILD_DIR)

$(BUILD_DIR):
	mkdir -p $@

$(TAR_DIR):
	mkdir -p $@
	
$(TEMPLATES_DIR):	$(BUILD_DIR)
	mkdir -p $@
	
