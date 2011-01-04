SRC = src

BUILD = build

JS_LIB_FILES = \
	$(SRC)/js/underscore.js

JS_CORE_FILES = \
	$(SRC)/js/find.js \


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

all: scss optimize

source: scss-watch serve-source

scss-watch: Makefile
	sass --scss --watch $(SRC)/scss:$(SRC)/css &
	
scss: Makefile
	sass --scss --update $(SRC)/scss:$(SRC)/css	

serve-build: Makefile
	$(shell cd $(BUILD); python -m SimpleHTTPServer)

serve-source: Makefile
	$(shell cd $(SRC); python -m SimpleHTTPServer)

optimize: Makefile
	requirejs-0.2.1/build/build.sh $(SRC)/js/app.build.js
	requirejs-0.2.1/build/build.sh $(SRC)/js/app.build.js


jsdoc: $(JS_FILES) Makefile
	rm -rf jsdoc
	$(JSDOC) -a -t=$(JSDOC_HOME)/templates/jsdoc -d=$@ -E="^pv-" $(JS_FILES)

clean:
	rm -rf $(BUILD) jsdoc
