#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#

#
# Copyright 2019 Joyent, Inc.
#

#
# Makefile: Electric Moray, a system for sharded Moray buckets
#

NAME = electric-moray

#
# Files
#
DOC_FILES =	 	index.md
JS_FILES :=		$(wildcard *.js) $(shell find lib test -name '*.js')
JSL_CONF_NODE =		tools/jsl.node.conf
JSL_FILES_NODE =	$(JS_FILES)
JSSTYLE_FILES =		$(JS_FILES)
JSSTYLE_FLAGS =		-f tools/jsstyle.conf
SMF_MANIFESTS_IN =	smf/manifests/haproxy.xml.in
BOOTSTRAP_MANIFESTS =	sapi_manifests/registrar/template

NODEUNIT_TESTS =	$(notdir $(wildcard test/*.test.js))

NODE_PREBUILT_VERSION =	v0.10.48
# sdc-*-multiarch 15.4.1.
NODE_PREBUILT_IMAGE	= 18b094b0-eb01-11e5-80c1-175dac7ddf02
ifeq ($(shell uname -s),SunOS)
	NODE_PREBUILT_TAG =	zone64
endif

ENGBLD_USE_BUILDIMAGE =	true
ENGBLD_REQUIRE := 	$(shell git submodule update --init deps/eng)
include ./deps/eng/tools/mk/Makefile.defs
TOP ?= $(error Unable to access eng.git submodule Makefiles.)

ifeq ($(shell uname -s),SunOS)
	include ./deps/eng/tools/mk/Makefile.node_prebuilt.defs
	include ./deps/eng/tools/mk/Makefile.agent_prebuilt.defs
else
	include ./deps/eng/tools/mk/Makefile.node.defs
endif
include ./deps/eng/tools/mk/Makefile.node_modules.defs
include ./deps/eng/tools/mk/Makefile.smf.defs

#
# MG Variables
#

RELEASE_TARBALL :=	$(NAME)-pkg-$(STAMP).tar.gz
ROOT :=			$(shell pwd)
RELSTAGEDIR :=		/tmp/$(NAME)-$(STAMP)

BASE_IMAGE_UUID = 04a48d7d-6bb5-4e83-8c3b-e60a99e0f48f
BUILDIMAGE_NAME = mantav1-electric-moray
BUILDIMAGE_DESC	= Manta moray proxy
BUILDIMAGE_PKGSRC = haproxy-1.6.2
AGENTS		= amon config registrar

#
# Repo-specific targets
#
.PHONY: all
all: $(SMF_MANIFESTS) $(BOOTSTRAP_MANIFESTS) $(STAMP_NODE_MODULES) manta-scripts

CLEAN_FILES += $(BOOTSTRAP_MANIFESTS)

.PHONY: manta-scripts
manta-scripts: deps/manta-scripts/.git
	mkdir -p $(BUILD)/scripts
	cp deps/manta-scripts/*.sh $(BUILD)/scripts

.PHONY: test
test: $(STAMP_NODE_MODULES) $(addprefix run-nodeunit.,$(NODEUNIT_TESTS))

run-nodeunit.%: test/%
	$(NODE) ./node_modules/.bin/nodeunit --reporter=tap $^

.PHONY: release
release: all
	@echo "Building $(RELEASE_TARBALL)"
	@mkdir -p $(RELSTAGEDIR)/root/opt/smartdc/electric-moray
	@mkdir -p $(RELSTAGEDIR)/root/opt/smartdc/boot
	@mkdir -p $(RELSTAGEDIR)/root/opt/smartdc/electric-moray/etc
	cp -r $(ROOT)/build \
	    $(ROOT)/bin \
	    $(ROOT)/boot \
	    $(ROOT)/lib \
	    $(ROOT)/main.js \
	    $(ROOT)/node_modules \
	    $(ROOT)/package.json \
	    $(ROOT)/sapi_manifests \
	    $(ROOT)/smf \
	    $(RELSTAGEDIR)/root/opt/smartdc/electric-moray/
	mv $(RELSTAGEDIR)/root/opt/smartdc/electric-moray/build/scripts \
	    $(RELSTAGEDIR)/root/opt/smartdc/electric-moray/boot
	ln -s /opt/smartdc/electric-moray/boot/configure.sh \
	    $(RELSTAGEDIR)/root/opt/smartdc/boot/configure.sh
	chmod 755 \
	    $(RELSTAGEDIR)/root/opt/smartdc/electric-moray/boot/configure.sh
	ln -s /opt/smartdc/electric-moray/boot/setup.sh \
	    $(RELSTAGEDIR)/root/opt/smartdc/boot/setup.sh
	chmod 755 $(RELSTAGEDIR)/root/opt/smartdc/electric-moray/boot/setup.sh
	cp $(ROOT)/etc/haproxy.cfg.in \
	    $(RELSTAGEDIR)/root/opt/smartdc/electric-moray/etc
	(cd $(RELSTAGEDIR) && $(TAR) -I pigz -cf $(ROOT)/$(RELEASE_TARBALL) root)
	@rm -rf $(RELSTAGEDIR)

# We include a pre-substituted copy of the template in our built image so that
# registrar doesn't go into maintenance on first boot. Then we ship both the
# .in file and this "bootstrap" substituted version. The boot/setup.sh script
# will perform this replacement again during the first boot, replacing @@PORTS@@
# with the real ports list.
sapi_manifests/registrar/template: sapi_manifests/registrar/template.in
	sed -e 's/@@PORTS@@/2020/g' $< > $@

.PHONY: publish
publish: release
	mkdir -p $(ENGBLD_BITS_DIR)/electric-moray
	cp $(ROOT)/$(RELEASE_TARBALL) \
	    $(ENGBLD_BITS_DIR)/$(NAME)/$(RELEASE_TARBALL)

include ./deps/eng/tools/mk/Makefile.deps
ifeq ($(shell uname -s),SunOS)
	include ./deps/eng/tools/mk/Makefile.node_prebuilt.targ
	include ./deps/eng/tools/mk/Makefile.agent_prebuilt.targ
else
	include ./deps/eng/tools/mk/Makefile.node.targ
endif
include ./deps/eng/tools/mk/Makefile.node_modules.targ
include ./deps/eng/tools/mk/Makefile.smf.targ
include ./deps/eng/tools/mk/Makefile.targ
