from datetime import UTC, datetime
from enum import StrEnum
from uuid import UUID

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, field_validator, model_validator


class EventType(StrEnum):
    VIEW_STARTED = "VIEW_STARTED"
    VIEW_FINISHED = "VIEW_FINISHED"
    VIEW_PAUSED = "VIEW_PAUSED"
    VIEW_RESUMED = "VIEW_RESUMED"
    LIKED = "LIKED"
    SEARCHED = "SEARCHED"


class DeviceType(StrEnum):
    MOBILE = "MOBILE"
    DESKTOP = "DESKTOP"
    TV = "TV"
    TABLET = "TABLET"


class Event(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    event_id: UUID
    user_id: str = Field(min_length=1, max_length=128)
    movie_id: str = Field(min_length=1, max_length=128)
    event_type: EventType
    timestamp: AwareDatetime
    device_type: DeviceType
    session_id: str = Field(min_length=1, max_length=128)
    progress_seconds: int | None = Field(default=None, strict=True, ge=0, le=2**31 - 1)

    @field_validator("timestamp", mode="before")
    @classmethod
    def require_datetime(cls, value):
        if isinstance(value, str):
            return datetime.fromisoformat(value)
        if not isinstance(value, datetime):
            raise ValueError("timestamp must be a timezone-aware datetime")
        return value

    @field_validator("timestamp")
    @classmethod
    def normalize_utc(cls, value):
        value = value.astimezone(UTC)
        if not 1970 <= value.year < 2100:
            raise ValueError("timestamp must be within 1970–2099")
        return value.replace(microsecond=value.microsecond // 1000 * 1000)

    @model_validator(mode="after")
    def validate_progress(self):
        is_view = self.event_type.startswith("VIEW_")
        if is_view != (self.progress_seconds is not None):
            raise ValueError("progress_seconds is required for views and null otherwise")
        return self
