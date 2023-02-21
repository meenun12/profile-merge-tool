from typing import Dict, Any
from fastapi import status
from fastapi.responses import JSONResponse


class ErrorResponse(JSONResponse):

    def __init__(self, exc: Exception, **kwargs):
        super().__init__(**self._parent_kwargs(exc, **kwargs))

    def _parent_kwargs(self, exc: Exception, **kwargs) -> Dict[str, Any]:
        status_code = kwargs.get("status_code")
        try:
            status_code = status_code if status_code else exc.status_code
        except AttributeError:
            status_code = status.HTTP_500_INTERNAL_SERVER_ERROR

        return {
            **kwargs,
            "status_code": status_code,
            "content": {
                "status_code": status_code,
                "error": True,
                "type": f"{exc.__class__.__name__}",
                "message": str(exc),
            },
        }
