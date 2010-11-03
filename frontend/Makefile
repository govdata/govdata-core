JS_CORE_FILES = \
	js/common.js \
	js/api.js \
	js/find.js \
	js/show.js \

JS_LIB_FILES = \
	js/lib/jquery.js

JS_MODULE_FILES = \
	js/modules/module.js


JS_FILES = \
	$(JS_CORE_FILES) \
	$(JS_LIB_FILES) \
	$(JS_MODULE_FILES)

JS_COMPILER = \
	java -jar lib/google-compiler/compiler-20100201.jar \
	--charset UTF-8 \
	--warning_level=QUIET

JSDOC_HOME = /Library/jsdoc-toolkit
JSDOC = java -jar $(JSDOC_HOME)/jsrun.jar $(JSDOC_HOME)/app/run.js

all: govdata-d0.1.js govdata-r0.1.js
govdata-d0.1.js: $(JS_FILES)
govdata-r0.1.js: $(JS_FILES)

%-d0.1.js: Makefile
	grep '	' -Hn $(filter %.js,$^) && echo "ERROR: tab" && exit 1 || true
	grep '' -Hn $(filter %.js,$^) && echo "ERROR: dos newline" && exit 1 || true
	grep ' $$' -Hn $(filter %.js,$^) && echo "ERROR: trailing space" && exit 1 || true
	rm -f $@
	echo "// $(shell git rev-parse HEAD)" >> $@
	cat $(filter %.js,$^) >> $@

%-r0.1.js:: Makefile
	rm -f $@
	echo "// $(shell git rev-parse --short HEAD)" >> $@
	cat $(filter %.js,$^) | $(JS_COMPILER) >> $@

jsdoc: $(JS_FILES) Makefile
	rm -rf jsdoc
	$(JSDOC) -a -t=$(JSDOC_HOME)/templates/jsdoc -d=$@ -E="^pv-" $(JS_FILES)

clean:
	rm -rf govdata-d0.1.js govdata-r0.1.js jsdoc
