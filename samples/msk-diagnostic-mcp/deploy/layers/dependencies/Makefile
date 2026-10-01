# SAM makefile build hook for the dependencies layer.
#
# Installs the runtime dependencies AND copies the sibling
# amazon_msk_diagnostic_mcp package (staged into this dir by ../../deploy.sh
# before `sam build` runs) into the layer's python/ directory so Lambda can
# import the tools without duplicating server.py.

build-DependenciesLayer:
	mkdir -p "$(ARTIFACTS_DIR)/python"
	python3 -m pip install -r requirements.txt --target="$(ARTIFACTS_DIR)/python"
	cp -r amazon_msk_diagnostic_mcp "$(ARTIFACTS_DIR)/python/"
