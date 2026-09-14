"""Minimal persistent asyncio HTTP/1.1 client used by the shared load core."""

import asyncio


class HttpError(RuntimeError):
    pass


class AsyncHttpConnection:
    def __init__(self, host, port, timeout, headers=()):
        self.host = host
        self.port = port
        self.timeout = timeout
        # Engine-constant headers (credentials); see driver.request_wire_bytes,
        # which emits the same set into the replay blob.
        self.extra = "".join(f"{name}: {value}\r\n" for name, value in headers)
        self.reader = None
        self.writer = None

    async def connect(self):
        # Callers hold the per-request deadline; no separate connect timeout.
        if self.writer is None or self.writer.is_closing():
            self.reader, self.writer = await asyncio.open_connection(self.host, self.port)

    async def close(self):
        if self.writer is not None:
            self.writer.close()
            try:
                await self.writer.wait_closed()
            except Exception:
                pass
        self.reader = self.writer = None

    async def _chunked_body(self):
        chunks = []
        while True:
            line = await self.reader.readline()
            size = int(line.split(b";", 1)[0], 16)
            if size == 0:
                while await self.reader.readline() not in (b"\r\n", b"\n", b""):
                    pass
                break
            chunks.append(await self.reader.readexactly(size))
            await self.reader.readexactly(2)
        return b"".join(chunks)

    async def request(self, request):
        # One deadline covers the whole exchange; per-read wait_for wrappers
        # were the driver's dominant per-request cost at high QPS.
        head = (f"{request.method} {request.path} HTTP/1.1\r\n"
                f"Host: {self.host}:{self.port}\r\n"
                "Content-Type: application/json\r\n"
                "Accept: application/json, application/x-ndjson\r\n"
                "Connection: keep-alive\r\n"
                f"{self.extra}"
                f"Content-Length: {len(request.body)}\r\n\r\n").encode()
        try:
            async with asyncio.timeout(self.timeout):
                await self.connect()
                self.writer.write(head + request.body)
                await self.writer.drain()
                block = await self.reader.readuntil(b"\r\n\r\n")
                lines = block.split(b"\r\n")
                status = int(lines[0].split(None, 2)[1])
                headers = {}
                for line in lines[1:]:
                    if not line:
                        continue
                    key, val = line.decode("latin1").split(":", 1)
                    headers[key.lower()] = val.strip().lower()
                if "chunked" in headers.get("transfer-encoding", ""):
                    body = await self._chunked_body()
                elif "content-length" in headers:
                    body = await self.reader.readexactly(int(headers["content-length"]))
                else:
                    body = await self.reader.read()
            if headers.get("connection") == "close":
                await self.close()
            if status < 200 or status >= 300:
                raise HttpError(f"HTTP {status}: {body[:1000].decode(errors='replace')}")
            return body
        except Exception:
            await self.close()
            raise
