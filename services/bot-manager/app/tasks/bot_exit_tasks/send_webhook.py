import os
from typing import Any, Optional

import httpx
from sqlalchemy.ext.asyncio import AsyncSession
from shared_models.models import Meeting, MeetingSession, User
from sqlalchemy import select
from pprint import pformat
from dependency_injector.wiring import inject, Provide
from app.core.protocols.logger_protocol import LoggerProtocol

DATA_KEYS_DUPLICATED_AT_TOP_LEVEL_OR_MEDIA = {
    "organization_id",
    "user_id",
    "audio_object_key",
    "video_object_key",
    "audio_content_type",
    "video_content_type",
    "audio_size_bytes",
    "video_size_bytes",
}


def _normalize_size_bytes(raw: Any) -> Optional[int]:
    """Coerce stored size (str/int) to int for structured media payloads."""
    if raw is None:
        return None
    try:
        return int(raw)
    except (TypeError, ValueError):
        return None


def _build_media_from_meeting_data(data: dict) -> dict:
    """
    Structured media artifacts for backend webhooks (replaces top-level audio_object_key / video_object_key).
    Keys still originate from meeting.data as written by bot exit callback or local fallback.
    """
    if not data:
        return {}
    media: dict = {}
    audio_key = data.get("audio_object_key")
    if audio_key:
        media["audio"] = {
            "object_key": audio_key,
            "content_type": data.get("audio_content_type") or data.get("content_type"),
            "size_bytes": _normalize_size_bytes(
                data.get("audio_size_bytes") or data.get("size_bytes")
            ),
        }
    video_key = data.get("video_object_key")
    if video_key:
        media["video"] = {
            "object_key": video_key,
            "content_type": data.get("video_content_type") or data.get("content_type"),
            "size_bytes": _normalize_size_bytes(
                data.get("video_size_bytes") or data.get("size_bytes")
            ),
        }
    return media


def _sanitize_payload_data(data: dict) -> dict:
    """
    Remove keys duplicated elsewhere in webhook payload (top-level/media).
    """
    if not data:
        return {}
    sanitized = dict(data)
    for key in DATA_KEYS_DUPLICATED_AT_TOP_LEVEL_OR_MEDIA:
        sanitized.pop(key, None)
    return sanitized


@inject
async def run(
    meeting: Meeting, 
    db: AsyncSession,
    logger: LoggerProtocol = Provide["logging.webhook_logger"]
):
    """
    Sends a webhook with the completed meeting details to a user-configured URL.
    """
    logger.info(f"Executing send_webhook task for meeting {meeting.id}")

    logger.debug(f"🔍 Meeting: {pformat(vars(meeting), indent=2)}")

    # The user should be loaded on the meeting object already by the task runner
    user = meeting.user
    if not user:
        error_msg = f"Could not find user on meeting object {meeting.id}"
        logger.error(error_msg)
        raise ValueError(error_msg)

    try:

        # Check if user has a webhook URL configured
        webhook_url = user.data.get('webhook_url') if user.data and isinstance(user.data, dict) else None

        if not webhook_url:
            logger.info(f"No webhook URL configured for user {user.email} (meeting {meeting.id})")
            return
        
        session_statement = (
            select(MeetingSession.session_uid)
            .where(MeetingSession.meeting_id == meeting.id)
            .order_by(MeetingSession.session_start_time.asc())
            .limit(1)
        )

        session_result = await db.execute(session_statement)
        connection_id = session_result.scalars().first() 

        data = meeting.data or {}
        sanitized_data = _sanitize_payload_data(data)
        media = _build_media_from_meeting_data(data)

        # Prepare the webhook payload: structured media.* (no top-level audio_object_key / video_object_key).
        payload = {
            'id': meeting.id,
            'user_id': data.get('user_id') if data.get('user_id') is not None else meeting.user_id,
            'platform': meeting.platform,
            'native_meeting_id': meeting.native_meeting_id,
            'constructed_meeting_url': meeting.constructed_meeting_url,
            'status': meeting.status,
            'bot_container_id': meeting.bot_container_id,
            'connection_id': connection_id if connection_id else None,
            'start_time': meeting.start_time.isoformat() if meeting.start_time else None,
            'end_time': meeting.end_time.isoformat() if meeting.end_time else None,
            'data': sanitized_data,
            'created_at': meeting.created_at.isoformat() if meeting.created_at else None,
            'updated_at': meeting.updated_at.isoformat() if getattr(meeting, "updated_at", None) else None,
            'participants': sanitized_data.get('participants', []),
        }
        payload['user_id_vexa'] = meeting.user_id
        if media:
            payload['media'] = media
        if data.get('organization_id') is not None:
            payload['organization_id'] = data['organization_id']
        if data.get('user_id') is not None:
            payload['user_id'] = data['user_id']

        # Send the webhook
        async with httpx.AsyncClient() as client:
            logger.info(f"Sending webhook to {webhook_url} for meeting {meeting.id}")
            logger.info(f"Webhook payload keys: {list(payload.keys())}")
            logger.info(f"Payload connection_id: {payload.get('connection_id')}")
            logger.info(f"Payload meeting status: {payload.get('status')}")
            
            headers = {'Content-Type': 'application/json'}
            webhook_secret = os.getenv('MEETING_WEBHOOK_SECRET')
            if webhook_secret:
                headers['X-Webhook-Secret'] = webhook_secret

            response = await client.post(
                webhook_url,
                json=payload,
                timeout=30.0,
                headers=headers
            )
            
            logger.info(f"Webhook response status: {response.status_code}")
            logger.info(f"Webhook response headers: {dict(response.headers)}")
            logger.info(f"Webhook response body: {response.text[:500]}")  # First 500 chars
            
            if response.status_code >= 200 and response.status_code < 300:
                logger.info(f"Successfully sent webhook for meeting {meeting.id} to {webhook_url}")
            else:
                logger.warning(f"Webhook for meeting {meeting.id} returned status {response.status_code}: {response.text}")

    except httpx.RequestError as e:
        logger.error(f"Failed to send webhook for meeting {meeting.id}: {e}")
    except Exception as e:
        logger.error(f"Unexpected error sending webhook for meeting {meeting.id}: {e}", exc_info=True) 