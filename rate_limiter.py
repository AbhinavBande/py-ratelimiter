'''
Rate Limiter
idea: build something that uses api similar to requests module but adds retry mechanism with rate limiting
notes:
    - get, post etc take in an additional param for rate limiting info.
    - enable global rate limiting by having an option to configure rate for a given endpoint
improvements:
    - _get_request_group should support pattern matching for url matching
    - support for multiple backoff strategies
'''
import time
import requests
import abc

class BaseRateLimitedRequest(abc.ABC):
    '''
        implement this class to create a custom rate limiting strategy
    '''
    @abc.abstractmethod
    def wait(self):
        pass

class LastRequestRateLimitedRequest(BaseRateLimitedRequest):
    def __init__(self, rate_limit=None):
        self.rate_limit = rate_limit
        self.last_request_time = 0

    def wait(self):
        current_time = time.time()
        elapsed = current_time - self.last_request_time
        if elapsed < self.rate_limit:
            time.sleep(self.rate_limit - elapsed)
        self.last_request_time = time.time()

class DefaultRateLimitedRequest(BaseRateLimitedRequest):
    def __init__(self, rate_limit=None):
        pass

    def wait(self):
        return

class RateLimiter:
    
    def __init__(self, rate_limit_strategy=DefaultRateLimitedRequest):
        self.session = requests.Session()
        self.endpoint_limits = {}
        self.rate_limit_strategy = rate_limit_strategy
    
    def _get_request_group(self, url):
        return self.endpoint_limits.get(url) # can be extended to support pattern matching

    def configure_limit(self, url, rate_limit):
        """
            accepts rate_limit as int or BaseRateLimitedRequest
            if int, it will use configured rate_limit_strategy
        """
        if isinstance(rate_limit, int):
            limiter = self.rate_limit_strategy(rate_limit=rate_limit)
        elif isinstance(rate_limit, BaseRateLimitedRequest):
            limiter = rate_limit

    def request(self, method, url, rate_limit=None, retries=1, **kwargs):
        limiter = self._get_request_group(url)
        
        if not limiter and rate_limit:
            limiter = self.rate_limit_strategy(rate_limit)
        
        if not limiter:
            limiter = DefaultRateLimitedRequest()

        for retry in range(retries):
            limiter.wait()
            try:
                return self.session.request(method, url, **kwargs)
            except requests.exceptions.RequestException as e:
                if retry == retries - 1:
                    raise e
                time.sleep(2 ** retry) # Exponential backoff


    def get(self, url, **kwargs):
        return self.request('GET', url, **kwargs)

    def post(self, url, **kwargs):
        return self.request('POST', url, **kwargs)

    def put(self, url, **kwargs):
        return self.request('PUT', url, **kwargs)

    def delete(self, url, **kwargs):
        return self.request('DELETE', url, **kwargs)