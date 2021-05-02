# Skytable
https://img.shields.io/github/license/Zomatree/skytable
This library is a wrapper around [Skytable](https://github.com/skytable/skytable)

# Installing
```bash
$ pip install git+https://github/com/Zomatree/skytable
```

# Usage

> Make sure you run the skytable server where `skytable.connect` is pointing to.

```python
import skytable
import asyncio

async def main():
    db = await skytable.connect("localhost", port=2003)

    await db.insert("my_key", "my_value")

    response = await db.get("my_key")

    print(response)

asyncio.run(main())
```
