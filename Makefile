#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#

#
# Copyright (c) 2017, Joyent, Inc.
#

#
# Makefile: Electric Moray, a system for sharded Moray buckets
#

#
# Files
#
JS_FILES :=		$(wildcard *.js) $(shell find lib test -name '*.js')
JSL_CONF_NODE =		tools/jsl.node.conf
JSL_FILES_NODE =	$(JS_FILES)
JSSTYLE_FILES =		$(JS_FILES)
JSSTYLE_FLAGS =		-f tools/jsstyle.conf
SMF_MANIFESTS_IN =	smf/manifests/haproxy.xml.in
BOOTSTRAP_MANIFESTS =	sapi_manifests/registrar/template

NODEUNIT_TESTS =	$(notdir $(wildcard test/*.test.js))


NODE_PREBUILT_VERSION =	v0.10.48
# Allow emoray builds on a VM other than sdc-multiarch/13.3.1.
NODE_PREBUILT_IMAGE =	b4bdc598-8939-11e3-bea4-8341f6861379
ifeq ($(shell uname -s),SunOS)
	NODE_PREBUILT_TAG =	zone64
endif

include ./tools/mk/Makefile.defs
ifeq ($(shell uname -s),SunOS)
	include ./tools/mk/Makefile.node_prebuilt.defs
else
	include ./tools/mk/Makefile.node.defs
endif
include ./tools/mk/Makefile.node_modules.defs
include ./tools/mk/Makefile.smf.defs

#
# MG Variables
#

RELEASE_TARBALL :=	electric-moray-pkg-$(STAMP).tar.bz2
ROOT :=			$(shell pwd)
RELSTAGEDIR :=		/tmp/$(STAMP)

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
	(cd $(RELSTAGEDIR) && $(TAR) -jcf $(ROOT)/$(RELEASE_TARBALL) root)
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
	@if [[ -z "$(BITS_DIR)" ]]; then \
		@echo "error: 'BITS_DIR' must be set for 'publish' target"; \
		exit 1; \
	fi
	mkdir -p $(BITS_DIR)/electric-moray
	cp $(ROOT)/$(RELEASE_TARBALL) \
	    $(BITS_DIR)/electric-moray/$(RELEASE_TARBALL)

include ./tools/mk/Makefile.deps
ifeq ($(shell uname -s),SunOS)
	include ./tools/mk/Makefile.node_prebuilt.targ
else
	include ./tools/mk/Makefile.node.targ
endif
include ./tools/mk/Makefile.node_modules.targ
include ./tools/mk/Makefile.smf.targ
include ./tools/mk/Makefile.targ
