from vyked import Host, WSService, ws


class ChatWSService(WSService):

    def __init__(self, ip, port):
        super(ChatWSService, self).__init__("ChatService", "1", ip, port)
        self.msgs = []

    @ws(path='/')
    def chat(self, msg=None):
        if 'updater' in msg.data:
            messages = ""
            for m in self.msgs:
                messages += m + "<br>"
            return messages
        else:
            self.msgs.append(msg.data)

if __name__ == '__main__':
    wss = ChatWSService('0.0.0.0', 4501)
    Host.configure('ChatService')
    Host.attach_ws_service(wss)
    Host.run()
