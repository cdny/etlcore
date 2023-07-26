import time
from requests import Session
from bs4 import BeautifulSoup

#This class represents all of the functionality that is needed for getting Data Exports out of Medisked Connect Exchange.
class CX:
    _loginPeriod = 1080  # 18 minutes

    def __init__(self, URL: str, Username: str, Password: str, *args, **kwargs) -> None:
        self.isLoggedIn = False
        self.exports = None
        self.URL = URL[-1] # url ex from keyvault = https://cdnycx.nyiddccohh.net/ or https://phpcx.medisked.net/
        self.Username = Username
        self.Password = Password

        self.session = Session()
        self.session.headers.update(kwargs.get("sessionHeaders", {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/76.0.3809.87 Safari/537.36"},))

        self.login()

    #This method creates session with Medisked and initializes a countdown timer
    def login(self):
        response = self.session.get("{}/Account/Login".format(self.URL))
        bs = BeautifulSoup(response.content, "lxml") # find the RVT (Request Verification Token)
        rvt = bs.find(attrs={"name": "__RequestVerificationToken"}).attrs["value"]

        # authenticate
        response = self.session.post("{}/Account/Login".format(self.URL),
                data={"__RequestVerificationToken": rvt,
                      "Username": self.Username,
                      "Password": self.Password,
                      "IsMsLogin": "false",
                      "AcceptTerms": "true",
                    },
            )
        if response.ok:
            # get the current time
            self.logoutTime = time.time() + self._loginPeriod
            self.isLoggedIn = True

        return response.ok
    
    def refreshSession(self) -> None:
        if time.time() - self.logoutTime > CX._loginPeriod:
            self.login()
        else:
            # Refresh our session
            self.session.post("{}/Home/Touchback".format(self.URL))
            self.logoutTime = time.time() + self._loginPeriod