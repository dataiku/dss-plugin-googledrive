PLUGIN_VERSION=1.0.1
PLUGIN_ID=googledrive

plugin:
	cat plugin.json|json_pp > /dev/null
	rm -rf dist
	mkdir dist
	zip --exclude "*.pyc" -r dist/dss-plugin-${PLUGIN_ID}-${PLUGIN_VERSION}.zip plugin.json python-lib custom-recipes python-fs-providers parameter-sets code-env
