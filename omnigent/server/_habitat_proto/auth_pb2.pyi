from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class UserIdentity(_message.Message):
    __slots__ = ("issuer", "subject", "email")
    ISSUER_FIELD_NUMBER: _ClassVar[int]
    SUBJECT_FIELD_NUMBER: _ClassVar[int]
    EMAIL_FIELD_NUMBER: _ClassVar[int]
    issuer: str
    subject: str
    email: str
    def __init__(self, issuer: _Optional[str] = ..., subject: _Optional[str] = ..., email: _Optional[str] = ...) -> None: ...

class StartLoginRequest(_message.Message):
    __slots__ = ("profile", "callback_uri", "force_reauthentication")
    PROFILE_FIELD_NUMBER: _ClassVar[int]
    CALLBACK_URI_FIELD_NUMBER: _ClassVar[int]
    FORCE_REAUTHENTICATION_FIELD_NUMBER: _ClassVar[int]
    profile: str
    callback_uri: str
    force_reauthentication: bool
    def __init__(self, profile: _Optional[str] = ..., callback_uri: _Optional[str] = ..., force_reauthentication: _Optional[bool] = ...) -> None: ...

class StartLoginResponse(_message.Message):
    __slots__ = ("authorization_url", "transaction_credential", "state", "expires_at_unix")
    AUTHORIZATION_URL_FIELD_NUMBER: _ClassVar[int]
    TRANSACTION_CREDENTIAL_FIELD_NUMBER: _ClassVar[int]
    STATE_FIELD_NUMBER: _ClassVar[int]
    EXPIRES_AT_UNIX_FIELD_NUMBER: _ClassVar[int]
    authorization_url: str
    transaction_credential: str
    state: str
    expires_at_unix: int
    def __init__(self, authorization_url: _Optional[str] = ..., transaction_credential: _Optional[str] = ..., state: _Optional[str] = ..., expires_at_unix: _Optional[int] = ...) -> None: ...

class CompleteLoginRequest(_message.Message):
    __slots__ = ("profile", "transaction_credential", "callback_uri", "state", "authorization_code")
    PROFILE_FIELD_NUMBER: _ClassVar[int]
    TRANSACTION_CREDENTIAL_FIELD_NUMBER: _ClassVar[int]
    CALLBACK_URI_FIELD_NUMBER: _ClassVar[int]
    STATE_FIELD_NUMBER: _ClassVar[int]
    AUTHORIZATION_CODE_FIELD_NUMBER: _ClassVar[int]
    profile: str
    transaction_credential: str
    callback_uri: str
    state: str
    authorization_code: str
    def __init__(self, profile: _Optional[str] = ..., transaction_credential: _Optional[str] = ..., callback_uri: _Optional[str] = ..., state: _Optional[str] = ..., authorization_code: _Optional[str] = ...) -> None: ...

class CompleteLoginResponse(_message.Message):
    __slots__ = ("session_credential", "identity", "client_token", "token_expires_at_unix", "session_expires_at_unix")
    SESSION_CREDENTIAL_FIELD_NUMBER: _ClassVar[int]
    IDENTITY_FIELD_NUMBER: _ClassVar[int]
    CLIENT_TOKEN_FIELD_NUMBER: _ClassVar[int]
    TOKEN_EXPIRES_AT_UNIX_FIELD_NUMBER: _ClassVar[int]
    SESSION_EXPIRES_AT_UNIX_FIELD_NUMBER: _ClassVar[int]
    session_credential: str
    identity: UserIdentity
    client_token: str
    token_expires_at_unix: int
    session_expires_at_unix: int
    def __init__(self, session_credential: _Optional[str] = ..., identity: _Optional[_Union[UserIdentity, _Mapping]] = ..., client_token: _Optional[str] = ..., token_expires_at_unix: _Optional[int] = ..., session_expires_at_unix: _Optional[int] = ...) -> None: ...

class GetClientTokenRequest(_message.Message):
    __slots__ = ("profile", "session_credential")
    PROFILE_FIELD_NUMBER: _ClassVar[int]
    SESSION_CREDENTIAL_FIELD_NUMBER: _ClassVar[int]
    profile: str
    session_credential: str
    def __init__(self, profile: _Optional[str] = ..., session_credential: _Optional[str] = ...) -> None: ...

class GetClientTokenResponse(_message.Message):
    __slots__ = ("client_token", "identity", "token_expires_at_unix", "session_expires_at_unix")
    CLIENT_TOKEN_FIELD_NUMBER: _ClassVar[int]
    IDENTITY_FIELD_NUMBER: _ClassVar[int]
    TOKEN_EXPIRES_AT_UNIX_FIELD_NUMBER: _ClassVar[int]
    SESSION_EXPIRES_AT_UNIX_FIELD_NUMBER: _ClassVar[int]
    client_token: str
    identity: UserIdentity
    token_expires_at_unix: int
    session_expires_at_unix: int
    def __init__(self, client_token: _Optional[str] = ..., identity: _Optional[_Union[UserIdentity, _Mapping]] = ..., token_expires_at_unix: _Optional[int] = ..., session_expires_at_unix: _Optional[int] = ...) -> None: ...

class GetSessionStatusRequest(_message.Message):
    __slots__ = ("profile", "session_credential")
    PROFILE_FIELD_NUMBER: _ClassVar[int]
    SESSION_CREDENTIAL_FIELD_NUMBER: _ClassVar[int]
    profile: str
    session_credential: str
    def __init__(self, profile: _Optional[str] = ..., session_credential: _Optional[str] = ...) -> None: ...

class GetSessionStatusResponse(_message.Message):
    __slots__ = ("identity", "session_expires_at_unix", "login_required")
    IDENTITY_FIELD_NUMBER: _ClassVar[int]
    SESSION_EXPIRES_AT_UNIX_FIELD_NUMBER: _ClassVar[int]
    LOGIN_REQUIRED_FIELD_NUMBER: _ClassVar[int]
    identity: UserIdentity
    session_expires_at_unix: int
    login_required: bool
    def __init__(self, identity: _Optional[_Union[UserIdentity, _Mapping]] = ..., session_expires_at_unix: _Optional[int] = ..., login_required: _Optional[bool] = ...) -> None: ...

class LogoutRequest(_message.Message):
    __slots__ = ("profile", "session_credential")
    PROFILE_FIELD_NUMBER: _ClassVar[int]
    SESSION_CREDENTIAL_FIELD_NUMBER: _ClassVar[int]
    profile: str
    session_credential: str
    def __init__(self, profile: _Optional[str] = ..., session_credential: _Optional[str] = ...) -> None: ...

class LogoutResponse(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...
