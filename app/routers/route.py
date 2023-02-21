# https://fastapi.tiangolo.com/advanced/custom-request-and-route/
# This is a way to overcome the fact that the function is designed to work using
# pubSub and we still need to be able to run it in the development envrironment.

import json
import base64
from typing import Callable, Union
from requests.exceptions import RetryError, ConnectionError, ReadTimeout
from pydantic import ValidationError
from fastapi import Request, Response
from fastapi.exceptions import RequestValidationError
from fastapi.routing import APIRoute
from app.responses import ErrorResponse
from app.logging import log


def _decode(data: Union[dict, bytes]):
    if not isinstance(data, dict):
        bytes_obj = base64.b64decode(data)
        return json.loads(bytes_obj)
    return data


class PushMessageRequest(Request):

    """Custom 'Request' class designed to handle incoming PubSub push messages.
    This message is available as a bytes string that decodes to, for example:
        ```
        {
            "message": {
                "data": "eyJqb2JfaWQiOiAiSElTVE9SWTAwMSIsICJkb21haW4iOiAiMC0xLTEwLnRlY2giLCAiZW52IjogImRldiJ9",
            },
            "attributes": {},
            "subscription": "123"
        }
        ```
    where the value at "message" > "data" is the data sent via PubSub and is a
    base-64-encoded string.

    The overloaded method implemented here gets that data and decodes it into a
    simple bytes string, which will then be used by the endpoint.
    """

    async def body(self) -> bytes:
        if not hasattr(self, "_body"):
            body_bytes = await super().body()
            body_obj = json.loads(body_bytes)
            # we assume body is in PubSub format if it has nested "message" > "data"
            data = body_obj.get("message", {}).get("data")
            if data:
                body_obj = _decode(data)
            self._body = json.dumps(body_obj).encode()
        return self._body


# register here all the exceptions that should return a custom error response
PUSH_MESSAGE_ROUTE_EXCEPTIONS = (
    RetryError,
    ConnectionError,
    ReadTimeout,
    ValidationError,
    RequestValidationError,
    UnicodeDecodeError,
    FileNotFoundError,
    ValueError,
)


class PushMessageRoute(APIRoute):

    """Custom 'APIRoute' class with 3 useful features:

    1. Use 'PushMessageRequest' to modify the encoded payload of a PubSub
       push message into the appropriate payload for the endpoint that uses
       this route.

    2. Register any exception that you want to be caught and return a custom
       response with code 200. This prevents the push message from being
       retried when it shouldn't be.

    3. Use the produced response to extract a log and write it to Cloud Run
       Logging and Firestore.
    """

    def get_route_handler(self) -> Callable:
        original_route_handler = super().get_route_handler()

        async def custom_route_handler(request: Request) -> Response:
            try:
                push_message_request = PushMessageRequest(
                    request.scope, request.receive
                )
                response = await original_route_handler(push_message_request)
            except PUSH_MESSAGE_ROUTE_EXCEPTIONS as exc:
                response = ErrorResponse(exc, status_code=200)
                request_body = await push_message_request.json()
                response_body = json.loads(response.body)
                log(payload={"request": request_body, "response": response_body})

            return response

        return custom_route_handler