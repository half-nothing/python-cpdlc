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

from python_cpdlc import CPDLC


async def main():
    # Create CPDLC client instance
    cpdlc = CPDLC()

    # Set your hoppie code
    cpdlc.set_logon_code("11111111111")

    # Set your email for network change (If you dont need to change network, you can skip it)
    cpdlc.set_email("halfnothingno@gmail.com")

    # of course, you can use your own hoppie server
    # cpdlc.set_acars_url("http://127.0.0.1:80")

    # you can add callback function which will be called when cpdlc connected and disconnected
    # there can only be one callback function per event
    # cpdlc.set_cpdlc_connect_callback(lambda: None)
    # cpdlc.set_cpdlc_disconnect_callback(lambda: None)
    # cpdlc.set_cpdlc_atc_info_update_callback(lambda: None)

    # you also can add message callback
    # cpdlc.add_message_sender_callback()
    # cpdlc.add_message_receiver_callback()

    # Decorators are recommended
    # @cpdlc.listen_message_receiver()
    # def message_receiver(msg: AcarsMessage):
    #     pass
    # @cpdlc.listen_message_sender()
    # def message_sender(to: str, msg: str):
    #     pass

    # you should set your callsign before you use CPDLC, and you can change this anytime you like
    # but if you change this callsign, you may miss some message send to you
    cpdlc.set_callsign("CES2352")

    # after set complete, you need to initialize service
    cpdlc.initialize_service()

    # you can reset service or reinitialize service anytime you like
    # cpdlc.reset_service()
    # cpdlc.reinitialize_service()

    # you can get your current network by cpdlc.network
    # you can change your network if necessary
    # cpdlc.change_network(Network.VATSIM)

    # some function...
    # cpdlc.query_info()
    # cpdlc.send_telex_message()
    # cpdlc.departure_clearance_delivery()

    # send login request
    cpdlc.cpdlc_login("ZSHA")

    # wait 60 seconds
    await asyncio.sleep(60)

    # request logout
    cpdlc.cpdlc_logout()


if __name__ == "__main__":
    asyncio.run(main())
```
