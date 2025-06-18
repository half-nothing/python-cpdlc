# python-cpdlc
A simple CPDLC client for flight simulation written by python

## Quick Start
1. install package with pip or any tools you like
```shell
pip install python-cpdlc
```
2. use example code under  
By the way, dont forgot to logon your ATC CPDLC first :)
```python
import asyncio

from python_cpdlc import CPDLC, Network

async def main():
    # Create CPDLC client with your email and hoppie code
    # Please dont use mine :(
    cpdlc = CPDLC("halfnothingno@gmail.com", "9BWovZBXLUy21m")
    # of course, you can use your own hoppie server
    # cpdlc = CPDLC("halfnothingno@gmail.com", "9BWovZBXLUy21m", "http://www.hoppie.nl/acars/system")
    
    # Set your callsign first, and you can change this anytime you like
    # But if you change this callsign, you may miss some message send to you
    cpdlc.set_callsign("CES2352")
    
    # You can change your network if necessary
    # You can got your current network by cpdlc.network
    cpdlc.change_network(Network.VATSIM)
    
    # Start poll thread for message reveiver
    # If you dont call this function you cant receive message
    await cpdlc.start_poller()
    
    # send login request
    cpdlc.cpdlc_login("ZSHA")
    # you can also send some other thing like DCL or just some message to someone
    
    # wait 60 seconds
    await asyncio.sleep(60)
    
    # request logout
    cpdlc.cpdlc_logout()

if __name__ == "__main__":
    asyncio.run(main())
```
