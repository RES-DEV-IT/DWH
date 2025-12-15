import requests

headers = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "*/*",
    "Accept-Encoding": "identity",
    "Connection": "close",
}

url = "https://v5.airtableusercontent.com/v3/u/48/48/1765814400000/371wVye0ME_kkLojB8wM2g/L9__JekWG8B0ddtndHmiWk5HdvvOhZravaaEGjCBw5zKebmZbcA4L7zTQANdB8mQKLy0SFqx0Q88PiEVj6Djoly1m31yHQ1F5x9WczIXI-5jmZ2nTW4llR5Cbf_Av8gAhuwy_-aT1E96UucCITWF92JXM3IM154Pi4pCZWBiMSx43HXZN74MQx7dtDYoM68h/FK2yL93BdXQ9_ffgcfuO04pYu1JzktZsCA1xYGWEZac"

with requests.get(url, stream=True, headers=headers, timeout=(10, 30)) as r:
    r.raise_for_status()
    with open("file.bin", "wb") as f:
        for chunk in r.iter_content(chunk_size=1024 * 1024):
            if chunk:
                f.write(chunk)