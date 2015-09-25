from vyked import Host, WSService, ws, WebSocketResponse
from aiohttp.web import MsgType


class ChatWSService(WSService):

    def __init__(self, ip, port):
        super(ChatWSService, self).__init__("ChatService", "1", ip, port)
        self.msgs = []

    @ws(path='/')
    def chat(self, request):
        wsk = WebSocketResponse()
        wsk.start(request)
        while True:
            msg = yield from wsk.receive()
            if msg.tp == MsgType.text:
                if msg.data == 'close':
                    yield from wsk.close()
                elif 'updater' in msg.data:
                    messages = ""
                    for m in self.msgs:
                        messages += m + "<br>"
                    wsk.send_str(messages)
                else:
                    self.msgs.append(msg.data)
        return wsk

if __name__ == '__main__':
    wss = ChatWSService('0.0.0.0', 4501)
    Host.registry_host = '127.0.0.1'
    Host.registry_port = 4500
    Host.pubsub_host = '127.0.0.1'
    Host.pubsub_port = 6379
    Host.name = 'Chat'
    Host.attach_service(wss)
    Host.run()
