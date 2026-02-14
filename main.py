# main.py
import os
import logging

# Flask မသုံးဘဲ simple HTTP server နဲ့သုံးမယ်
from http.server import HTTPServer, BaseHTTPRequestHandler

class HealthCheckHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header('Content-type', 'text/html')
        self.end_headers()
        self.wfile.write(b"Bot is ready for new code!")

if __name__ == "__main__":
    port = int(os.getenv("PORT", 10000))
    server = HTTPServer(("0.0.0.0", port), HealthCheckHandler)
    print(f"Starting server on port {port}...")
    server.serve_forever()