from tiled.server.authentication import Mode
from tiled.server.protocols import UserSessionState


class ScicatAuthenticator():
    mode = Mode.password

    def __init__(self) -> None:
        pass

    async def authenticate(self, username, password) -> UserSessionState:
        # communicate with scicat and test that the username and token match
        # and that the token is valid
        
        return UserSessionState(username, {"scicat_token": password})
        