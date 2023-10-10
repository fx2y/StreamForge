from http.server import BaseHTTPRequestHandler, HTTPServer


class HTTPAdapter:
    def __init__(self, port):
        self.port = port

    def start(self):
        server = HTTPServer(('', self.port), HTTPRequestHandler)
        server.serve_forever()


class HTTPRequestHandler(BaseHTTPRequestHandler):
    def do_POST(self):
        content_length = int(self.headers['Content-Length'])
        data = self.rfile.read(content_length)
        # Add deserialization logic here
        self.send_response(200)
        self.end_headers()


if __name__ == '__main__':
    http_adapter = HTTPAdapter(8080)
    http_adapter.start()
