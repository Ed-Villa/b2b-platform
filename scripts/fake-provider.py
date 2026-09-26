"""Controlled HTTP failures for manual experiments. Never part of production services.
Example: python scripts/fake-provider.py --port 8082 --fail-first 2 --status 503
Set PRODUCTS_URL=http://host.docker.internal:8082 for a locally running worker/container.
"""
import argparse
import json
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Lock
from urllib.parse import urlparse

parser = argparse.ArgumentParser()
parser.add_argument('--port', type=int, default=8082)
parser.add_argument('--fail-first', type=int, default=2)
parser.add_argument('--status', type=int, default=503)
parser.add_argument('--delay-ms', type=int, default=0)
args = parser.parse_args()
counts = {};
lock = Lock()


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        key = self.headers.get('X-Event-Id', 'unknown') + self.path
        with lock:
            counts[key] = counts.get(key, 0) + 1;attempt = counts[key]
        time.sleep(args.delay_ms / 1000)
        status = args.status if attempt <= args.fail_first else 200
        path = urlparse(self.path).path;
        identifier = path.rsplit('/', 1)[-1]
        body = ({'clientId': identifier, 'name': 'Fake client', 'status': 'ACTIVE', 'segment': 'WHOLESALE',
                 'taxRegime': 'GENERAL', 'market': 'MX'} if path.startswith('/clients/') else {'productId': identifier,
                                                                                               'name': 'Fake product',
                                                                                               'sku': 'FAKE',
                                                                                               'status': 'ACTIVE',
                                                                                               'taxCategory': 'STANDARD'})
        if status != 200: body = {'code': 'SIMULATED_FAILURE', 'message': 'Controlled test failure',
                                  'correlationId': self.headers.get('X-Correlation-Id', 'unknown')}
        data = json.dumps(body).encode()
        self.send_response(status);
        self.send_header('Content-Type', 'application/json');
        self.send_header('Content-Length', str(len(data)));
        self.end_headers()
        try:
            self.wfile.write(data)
        except (BrokenPipeError, ConnectionResetError):
            pass

    def log_message(self, fmt, *values):
        print(json.dumps({'component': 'fake-provider', 'message': fmt % values}))


print('Fake provider listening on', args.port, flush=True)
ThreadingHTTPServer(('0.0.0.0', args.port), Handler).serve_forever()
