from __future__ import annotations

import json
import logging
import random
import time
from typing import Dict, Optional

import httpx

from threads_github_bot.config import Settings
from threads_github_bot.models import PublishResult, PublishThreadResult, ThreadPublishPostResult

LOGGER = logging.getLogger(__name__)


class ThreadsPublisherClient:
    def __init__(self, settings: Settings, http_client: Optional[httpx.Client] = None) -> None:
        self.settings = settings
        if http_client is not None:
            self.http_client = http_client
            self._owns_client = False
        else:
            self.http_client = httpx.Client(
                base_url=settings.threads.base_url,
                timeout=settings.threads.timeout_seconds,
            )
            self._owns_client = True

    def close(self) -> None:
        if self._owns_client:
            self.http_client.close()

    def publish_text(self, text: str) -> PublishResult:
        thread_result = self.publish_thread([text])
        if not thread_result.posts:
            return PublishResult(success=False, error=thread_result.error)
        first = thread_result.posts[0]
        return PublishResult(
            success=thread_result.success,
            container_id=first.container_id,
            media_id=first.media_id,
            response=first.response,
            error=first.error or thread_result.error,
            status_code=first.status_code,
        )

    def publish_thread(self, posts) -> PublishThreadResult:
        if not self.settings.threads.access_token or not self.settings.threads.user_id:
            return PublishThreadResult(
                success=False,
                posts=[],
                error="Missing THREADS_ACCESS_TOKEN or THREADS_USER_ID",
            )
        results = []
        parent_media_id = None
        for _index, text in enumerate(posts, start=1):
            result = self._publish_single_post(text, reply_to_id=parent_media_id)
            results.append(result)
            if not result.success:
                return PublishThreadResult(success=False, posts=results, error=result.error)
            parent_media_id = result.media_id
        return PublishThreadResult(success=True, posts=results)

    def _publish_single_post(self, text: str, reply_to_id: Optional[str] = None) -> ThreadPublishPostResult:
        create_payload = {
            "media_type": "TEXT",
            "text": text,
            "access_token": self.settings.threads.access_token,
        }
        if reply_to_id:
            create_payload["reply_to_id"] = reply_to_id
        create_response, request_error = self._post_with_retry(
            "/{0}/threads".format(self.settings.threads.user_id),
            create_payload,
        )
        if create_response is None:
            return ThreadPublishPostResult(success=False, reply_to_id=reply_to_id, error=request_error)
        if create_response.status_code >= 400:
            return ThreadPublishPostResult(
                success=False,
                reply_to_id=reply_to_id,
                error=self._sanitize_text(create_response.text),
                status_code=create_response.status_code,
                response=self._sanitize_value(self._safe_json(create_response)),
            )

        create_body = create_response.json()
        container_id = create_body.get("id")
        if not container_id:
            return ThreadPublishPostResult(
                success=False,
                reply_to_id=reply_to_id,
                error="Threads create response missing id",
                response=self._sanitize_value(create_body),
            )

        publish_delay_seconds = self._choose_publish_delay_seconds()
        if publish_delay_seconds > 0:
            time.sleep(publish_delay_seconds)

        publish_payload = {
            "creation_id": container_id,
            "access_token": self.settings.threads.access_token,
        }
        publish_response, request_error = self._post_with_retry(
            "/{0}/threads_publish".format(self.settings.threads.user_id),
            publish_payload,
        )
        if publish_response is None:
            return ThreadPublishPostResult(
                success=False,
                container_id=container_id,
                reply_to_id=reply_to_id,
                error=request_error,
            )
        if publish_response.status_code >= 400:
            return ThreadPublishPostResult(
                success=False,
                container_id=container_id,
                reply_to_id=reply_to_id,
                error=self._sanitize_text(publish_response.text),
                status_code=publish_response.status_code,
                response=self._sanitize_value(self._safe_json(publish_response)),
            )

        publish_body = publish_response.json()
        return ThreadPublishPostResult(
            success=True,
            container_id=container_id,
            media_id=publish_body.get("id"),
            reply_to_id=reply_to_id,
            response=self._sanitize_value({"create": create_body, "publish": publish_body}),
        )

    def _safe_json(self, response: httpx.Response) -> Dict:
        try:
            return response.json()
        except ValueError:
            return {"text": response.text}

    def _post_with_retry(self, path: str, data: Dict[str, str]):
        last_error = None
        for attempt in range(1, self.settings.threads.retry_count + 1):
            try:
                response = self.http_client.post(path, data=data)
            except httpx.HTTPError as exc:
                last_error = "{0}: {1}".format(exc.__class__.__name__, exc)
                LOGGER.warning(
                    "threads_request_retry",
                    extra={"path": path, "attempt": attempt, "error": last_error},
                )
            else:
                if response.status_code < 500:
                    return response, None
                last_error = "HTTP {0}".format(response.status_code)
                LOGGER.warning(
                    "threads_server_retry",
                    extra={"path": path, "attempt": attempt, "status_code": response.status_code},
                )
                if attempt == self.settings.threads.retry_count:
                    return response, None
            time.sleep(self.settings.threads.retry_backoff_seconds * attempt)
        return None, last_error or "Unknown Threads request failure"

    def _choose_publish_delay_seconds(self) -> int:
        minimum = max(0, self.settings.threads.publish_delay_seconds_min)
        maximum = max(minimum, self.settings.threads.publish_delay_seconds_max)
        if maximum <= 0:
            return 0
        if minimum == maximum:
            return minimum
        return random.randint(minimum, maximum)

    def _sanitize_text(self, value: str) -> str:
        token = self.settings.threads.access_token or ""
        if token:
            return value.replace(token, "***")
        return value

    def _sanitize_value(self, value):
        token = self.settings.threads.access_token or ""
        if not token:
            return value
        serialized = json.dumps(value)
        return json.loads(serialized.replace(token, "***"))
