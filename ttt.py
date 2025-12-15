import os, time, requests

url = "https://v5.airtableusercontent.com/v3/u/48/48/1765814400000/371wVye0ME_kkLojB8wM2g/L9__JekWG8B0ddtndHmiWk5HdvvOhZravaaEGjCBw5zKebmZbcA4L7zTQANdB8mQKLy0SFqx0Q88PiEVj6Djoly1m31yHQ1F5x9WczIXI-5jmZ2nTW4llR5Cbf_Av8gAhuwy_-aT1E96UucCITWF92JXM3IM154Pi4pCZWBiMSx43HXZN74MQx7dtDYoM68h/FK2yL93BdXQ9_ffgcfuO04pYu1JzktZsCA1xYGWEZac"  # твоя ссылка
path = "POP49-DL.xlsx"

headers_base = {
    "User-Agent": "curl/8.5.0",       # можно любой нормальный
    "Accept": "*/*",
    "Accept-Encoding": "identity",
    "Connection": "close",
}

for attempt in range(20):
    downloaded = os.path.getsize(path) if os.path.exists(path) else 0
    headers = dict(headers_base)
    if downloaded:
        headers["Range"] = f"bytes={downloaded}-"

    try:
        with requests.get(url, stream=True, headers=headers, timeout=(10, 60)) as r:
            if r.status_code not in (200, 206):
                r.raise_for_status()

            mode = "ab" if (downloaded and r.status_code == 206) else "wb"
            with open(path, mode) as f:
                for chunk in r.iter_content(chunk_size=16 * 1024):
                    if chunk:
                        f.write(chunk)

        # проверка что докачали все (если сервер дал длину)
        final = os.path.getsize(path)
        print("Downloaded:", final, "bytes")
        break

    except (requests.ReadTimeout, requests.ConnectionError) as e:
        time.sleep(1 + attempt)  # backoff
else:
    raise RuntimeError("Не удалось скачать после 20 попыток")
