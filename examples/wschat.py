from vyked import Host, HTTPService, websocket


class ChatService(HTTPService):

    def __init__(self, ip, port):
        super(ChatService, self).__init__("ChatService", "1", ip, port)
        self.msgs = []

    @websocket(path='/')
    def chat(self, msg=None):
        if 'updater' in msg.data:
            messages = "<br>".join(self.msgs) + "<br>"
            return messages
        else:
            self.msgs.append(msg.data)

if __name__ == '__main__':
    wss = ChatService('0.0.0.0', 4501)
    Host.configure('ChatService')
    Host.attach_http_service(wss)
    Host.run()
