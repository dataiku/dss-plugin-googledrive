# Fix for ImportError messages https://github.com/googleapis/google-api-python-client/issues/325


class MemoryCache():
    _CACHE = {}

    def get(self, url):
        return MemoryCache._CACHE.get(url)

    def set(self, url, content):
        MemoryCache._CACHE[url] = content
