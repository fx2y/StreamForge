import json
from http.server import BaseHTTPRequestHandler, HTTPServer

from gunicorn.app.base import BaseApplication


class HTTPAdapter:
    def __init__(self, port):
        self.port = port

    def start(self):
        class HTTPApplication(BaseApplication):
            def __init__(self, app, options=None):
                self.options = options or {}
                self.application = app
                super().__init__()

            def load_config(self):
                for key, value in self.options.items():
                    if key in self.cfg.settings and value is not None:
                        self.cfg.set(key.lower(), value)

            def load(self):
                return self.application

        options = {
            'bind': f'0.0.0.0:{self.port}',
            'workers': 4,  # adjust this value based on your system resources
            'worker_class': 'sync',  # use sync worker class for simplicity
        }

        # use gunicorn
        HTTPApplication(HTTPServer((options['bind'].split(':')[0], int(options['bind'].split(':')[1])), HTTPHandler),
                        options).run()


class HTTPHandler(BaseHTTPRequestHandler):
    def deserialize_data(self, data):
        # Add deserialization logic here
        return json.loads(data.decode())

    def do_POST(self):
        content_length = int(self.headers['Content-Length'])
        data = self.rfile.read(content_length)
        # Add deserialization logic here
        try:
            deserialized_data = self.deserialize_data(data)
            self.send_response(200)
        except Exception as e:
            self.send_response(400)
            self.send_header('Content-type', 'text/plain')
            self.end_headers()
            self.wfile.write(str(e).encode())
        else:
            self.end_headers()


if __name__ == '__main__':
    http_adapter = HTTPAdapter(8080)
    http_adapter.start()
