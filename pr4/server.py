from functools import partial
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
import time


ROOT = Path(__file__).resolve().parent
DELAY = 0.2


class Handler(SimpleHTTPRequestHandler):
    def do_GET(self):
        # Имитируем задержку ответа удалённого сервера.
        time.sleep(DELAY)
        super().do_GET()

    def log_message(self, format, *args):
        # Не засоряем терминал сообщениями о каждом запросе.
        pass


if __name__ == "__main__":
    handler = partial(Handler, directory=str(ROOT / "web"))
    server = ThreadingHTTPServer(("127.0.0.1", 8000), handler)

    print("Сервер: http://127.0.0.1:8000", flush=True)
    print(f"Задержка каждого ответа: {DELAY} с", flush=True)
    print("Для остановки нажми Ctrl+C", flush=True)

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nСервер остановлен")
    finally:
        server.server_close()
