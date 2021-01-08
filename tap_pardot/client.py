import backoff
import requests
import singer
from singer import utils
import redis
from datetime import timedelta
import json

LOGGER = singer.get_logger()

AUTH_URL = "https://pi.pardot.com/api/login/version/3"
ENDPOINT_BASE = "https://pi.pardot.com/api/"


class Pardot5xxError(Exception):
    pass


class PardotException(Exception):
    def __init__(self, message, response_content):
        self.code = response_content.get("@attributes", {}).get("err_code")
        self.response = response_content
        super().__init__(message)


class PardotUserLimitReached(Exception):
    def __init__(self):
        self.code = 1
        err = {
            "message": "Pardot API limit set in config has been met.",
            "code": 1
        }
        self.response = f"Error: {json.dumps(err)}"
        super().__init__(self.response)


def is_not_retryable_pardot_exception(exc):
    if isinstance(exc, Pardot5xxError):
        return False
    if isinstance(exc, PardotUserLimitReached):
        return True
    if exc.code == 66:
        LOGGER.warn("Exceeded concurrent request limit, backing off exponentially.")
        return False
    return True


class Client:
    """Lightweight Client wrapper to allow switching between version 3 and 4 API based
    on availability, if desired."""

    api_version = None
    api_key = None
    creds = None

    get_url = "{}/version/{}/do/query"
    describe_url = "{}/version/{}/do/describe"

    def __init__(self, config):
        self.config = config
        self.creds = {
            "email": self.config["email"],
            "password": self.config["password"],
            "user_key": self.config["user_key"]
        }
        self.api_calls_limit = self.config.get("daily_limit", None)
        self.redis = redis.Redis(
            host = self.config["redis"]["host"],
            port = self.config["redis"]["port"],
            db = self.config["redis"]["db"]
        )
        date_today = utils.now().strftime("%Y-%m-%d")
        self.redis_key = f"tap-pardot/{self.config['account_id']}/{date_today}"
        
        curr_api_calls_made = self.redis.get(self.redis_key)
        if not curr_api_calls_made:
            self.redis.set(
                name = self.redis_key,
                value = 0,
                ex = timedelta(days = self.config["redis"].get("ttl_days", 30))
            )
            num_api_calls_made = 0
        else:
            num_api_calls_made = int(curr_api_calls_made)
        
        if self.api_calls_limit and num_api_calls_made >= self.api_calls_limit:
            raise PardotUserLimitReached()
        
        self.login()

    def login(self):
        response = requests.post(
            AUTH_URL,
            data=self.creds,
            params={"format": "json"},
        )
        
        # This will only work if they use HTTP codes. Handling Pardot
        # errors below.
        response.raise_for_status()
        
        content = response.json()

        self._check_error(content, "authenticating")

        self.redis.incr(self.redis_key)
        self.api_version = content.get("version") or "3"
        self.api_key = content["api_key"]

    def _check_error(self, content, activity):
        error_message = content.get("err")
        if error_message:
            error_code = content["@attributes"]["err_code"]
            raise PardotException(
                "Pardot returned error code {} while {}. Message: {}".format(
                    error_code, activity, error_message
                ),
                content,
            )

    def _get_auth_header(self):
        return {
            "Authorization": "Pardot api_key={}, user_key={}".format(
                self.api_key, self.creds["user_key"]
            )
        }

    def _make_request(self, method, url, params=None):
        num_api_calls_made = int(self.redis.get(self.redis_key))

        if self.api_calls_limit and num_api_calls_made < self.api_calls_limit:
            LOGGER.info(
                "%s - Making request to %s endpoint %s, with params %s",
                url,
                method.upper(),
                url,
                params,
            )
            response = requests.request(
                method, url, headers=self._get_auth_header(), params=params
            )

            # 5xx errors should be retried
            if response.status_code >= 500:
                raise Pardot5xxError()

            response.raise_for_status()

            content = response.json()
            error_message = content.get("err")

            if error_message:
                error_code = content["@attributes"]["err_code"]

                # Error code 1 indicates a bad api_key or user_key
                # If we get error code 1 then re-authenticate login
                # http://developer.pardot.com/kb/error-codes-messages/#error-code-1

                if error_code == 1:
                    LOGGER.info("API key or user key expired -- Reauthenticating once")
                    self.login()
                    response = requests.request(
                        method, url, headers=self._get_auth_header(), params=params
                    )
                    content = response.json()
                    self.redis.incr(self.redis_key)
                
                if error_code == 122:
                    raise PardotUserLimitReached()
            
            else:
                self.redis.incr(self.redis_key)

            return content
        else:
            raise PardotUserLimitReached()


    @backoff.on_exception(
        backoff.expo,
        (PardotException,Pardot5xxError),
        giveup=is_not_retryable_pardot_exception,
        jitter=None,
    )
    def describe(self, endpoint, **kwargs):
        url = (ENDPOINT_BASE + self.describe_url).format(endpoint, self.api_version)

        params = {"format": "json", "output": "bulk", **kwargs}

        content = self._make_request("get", url, params)

        self._check_error(content, "describing endpoint")

        return content

    @backoff.on_exception(
        backoff.expo,
        (PardotException,Pardot5xxError),
        giveup=is_not_retryable_pardot_exception,
        jitter=None,
    )
    def _fetch(self, method, endpoint, format_params, **kwargs):
        base_formatting = [endpoint, self.api_version]
        if format_params:
            base_formatting.extend(format_params)
        url = (ENDPOINT_BASE + self.get_url).format(*base_formatting)

        params = {"format": "json", "output": "bulk", **kwargs}

        content = self._make_request(method, url, params)

        self._check_error(content, "retrieving endpoint")

        return content

    def get(self, endpoint, format_params=None, **kwargs):
        return self._fetch("get", endpoint, format_params, **kwargs)

    def post(self, endpoint, format_params=None, **kwargs):
        return self._fetch("post", endpoint, format_params, **kwargs)
