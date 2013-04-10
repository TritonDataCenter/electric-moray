#
# Copyright (c) 2012, Joyent, Inc. All rights reserved.
#
# Makefile: basic Makefile for template API service
#
# This Makefile is a template for new repos. It contains only repo-specific
# logic and uses included makefiles to supply common targets (javascriptlint,
# jsstyle, restdown, etc.), which are used by other repos as well. You may well
# need to rewrite most of this file, but you shouldn't need to touch the
# included makefiles.
#
# If you find yourself adding support for new targets that could be useful for
# other projects too, you should add these to the original versions of the
# included Makefiles (in eng.git) so that other teams can use them too.
#

#
# Tools
#
NODEUNIT	:= ./node_modules/.bin/nodeunit
NODECOVER	:= ./node_modules/.bin/cover
BUNYAN		:= ./node_modules/.bin/bunyan
JSONTOOL	:= ./node_modules/.bin/json

#
# Files
#
DOC_FILES	 = index.restdown boilerplateapi.restdown
JS_FILES	:= $(shell ls *.js) $(shell find lib test -name '*.js')
JSL_CONF_NODE	 = tools/jsl.node.conf
JSL_FILES_NODE   = $(JS_FILES)
JSSTYLE_FILES	 = $(JS_FILES)
JSSTYLE_FLAGS    = -f tools/jsstyle.conf
REPO_MODULES	 = src/node-dummy
SMF_MANIFESTS_IN = smf/manifests/haproxy.xml.in


NODE_PREBUILT_VERSION=v0.8.22

ifeq ($(shell uname -s),SunOS)
	NODE_PREBUILT_CC_VERSION=4.6.2
	NODE_PREBUILT_TAG=zone
endif

include ./tools/mk/Makefile.defs
ifeq ($(shell uname -s),SunOS)
	include ./tools/mk/Makefile.node_prebuilt.defs
else
	include ./tools/mk/Makefile.node.defs
endif
include ./tools/mk/Makefile.smf.defs

#
# MG Variables
#

RELEASE_TARBALL         := electric-moray-pkg-$(STAMP).tar.bz2
ROOT                    := $(shell pwd)
TMPDIR                  := /tmp/$(STAMP)

#
# Repo-specific targets
#
.PHONY: all
all: $(SMF_MANIFESTS) | $(REPO_DEPS) $(NODEUNIT)
	$(NPM) rebuild

$(NODEUNIT): | $(NPM_EXEC)
	$(NPM) install

CLEAN_FILES += $(TAP) ./node_modules/tap

.PHONY: test
test: | $(NODEUNIT)
	$(NODEUNIT) test/buckets.test.js | $(BUNYAN)
	$(NODEUNIT) test/objects.test.js | $(BUNYAN)
	$(NODEUNIT) test/integ.test.js | $(BUNYAN)

.PHONY: release
release: all docs $(SMF_MANIFESTS)
	@echo "Building $(RELEASE_TARBALL)"
	@mkdir -p $(TMPDIR)/root/opt/smartdc/electric-moray
	@mkdir -p $(TMPDIR)/root
	@mkdir -p $(TMPDIR)/root/opt/smartdc/electric-moray/etc
	cp -r $(ROOT)/build \
		$(ROOT)/lib \
		$(ROOT)/main.js \
		$(ROOT)/node_modules \
		$(ROOT)/package.json \
		$(ROOT)/smf \
		$(TMPDIR)/root/opt/smartdc/electric-moray/
	cp $(ROOT)/etc/config.json.in $(TMPDIR)/root/opt/smartdc/electric-moray/etc
	cp $(ROOT)/etc/haproxy.cfg.in $(TMPDIR)/root/opt/smartdc/electric-moray/etc
	(cd $(TMPDIR) && $(TAR) -jcf $(ROOT)/$(RELEASE_TARBALL) root)
	@rm -rf $(TMPDIR)

.PHONY: publish
publish: release
	@if [[ -z "$(BITS_DIR)" ]]; then \
		@echo "error: 'BITS_DIR' must be set for 'publish' target"; \
		exit 1; \
	fi
	mkdir -p $(BITS_DIR)/electric-moray
	cp $(ROOT)/$(RELEASE_TARBALL) $(BITS_DIR)/electric-moray/$(RELEASE_TARBALL)

include ./tools/mk/Makefile.deps
ifeq ($(shell uname -s),SunOS)
	include ./tools/mk/Makefile.node_prebuilt.targ
else
	include ./tools/mk/Makefile.node.targ
endif
include ./tools/mk/Makefile.smf.targ
include ./tools/mk/Makefile.targ
