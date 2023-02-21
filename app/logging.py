"""
This module defines a function that is useful to log simple or structured logs
to Cloud Logging on Cloud Run.

It uses the fact that Logging Agent is enabled on Cloud Run, which takes in the
print outs from an app, parses them and then makes them available in fields
"textPayload" or "jsonPayload" (depending on what is being logged) and also
sets a severity level for that log.

More info:
* https://cloud.google.com/logging/docs/structured-logging#special-payload-fields and refs therein
* learn how to search logs using the logging query language https://cloud.google.com/logging/docs/view/logging-query-language
"""

import json
from typing import Optional


def log(
    payload: Optional[dict] = None,
    message: Optional[str] = None,
    severity: str = "DEBUG",
) -> None:
    """Log to Cloud Logging.
    Args:
        payload: if provided, this will appear under field 'jsonPayload'.
        message: if provided, this will appear under field 'textPayload'.
        severity: https://cloud.google.com/logging/docs/reference/v2/rest/v2/LogEntry#logseverity
    """
    if payload:
        print(json.dumps({**payload, "severity": severity}))

    if message:
        print(json.dumps({"message": message, "severity": severity}))
