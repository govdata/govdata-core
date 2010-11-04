SRC = src

BUILD = build

JS_LIB_FILES = \
	$(SRC)/js/lib/underscore.js

JS_CORE_FILES = \
	$(SRC)/js/find.js \

JS_MODULE_FILES = \
	$(SRC)/js/modules/module.js


JS_FILES = \
	$(JS_LIB_FILES) \
	$(JS_CORE_FILES) \
	$(JS_MODULE_FILES)

JS_COMPILER = \
	java -jar lib/google-compiler/compiler-20100201.jar \
	--charset UTF-8 \
	--warning_level=QUIET

JSDOC_HOME = /Library/jsdoc-toolkit
JSDOC = java -jar $(JSDOC_HOME)/jsrun.jar $(JSDOC_HOME)/app/run.js

all: createfolders scss copyfiles govdata.js
govdata.js: $(JS_FILES)

%data.js:: Makefile
	rm -f $(BUILD)/$@
	echo "// $(shell git rev-parse --short HEAD)" >> $(BUILD)/$@
	cat $(filter %.js,$^) | $(JS_COMPILER) >> $(BUILD)/$@

scss-watch: Makefile
	sass --scss --watch $(SRC)/scss:$(SRC)/css

serve: Makefile
	$(shell cd $(SRC); python -m SimpleHTTPServer)

scss: Makefile
	sass --scss --update $(SRC)/scss:$(BUILD)/css

createfolders: Makefile
	mkdir -p $(BUILD)

copyfiles: Makefile
	cp $(SRC)/index-release.html $(BUILD)/index.html
	cp $(SRC)/favicon.ico $(BUILD)/favicon.ico
	cp -r $(SRC)/images $(BUILD)/images

jsdoc: $(JS_FILES) Makefile
	rm -rf jsdoc
	$(JSDOC) -a -t=$(JSDOC_HOME)/templates/jsdoc -d=$@ -E="^pv-" $(JS_FILES)

clean:
	rm -rf $(BUILD) jsdoc
