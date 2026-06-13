from dataclasses import dataclass, field
from typing import List, Optional

from pydantic import BaseModel


@dataclass
class JWTClaims:
    sub: str
    org_id: str
    email: str = ""
    org_name: str = ""
    license_type: str = "free"
    quota_tier: str = "free"
    roles: List[str] = field(default_factory=list)


class ErrorEnvelope(BaseModel):
    error: str
    message: str
    request_id: Optional[str] = None
