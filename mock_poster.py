import aiohttp
import asyncio
import async_timeout

def get_json_string(file):
    return file.readline()

async def post(session, url, msg):
    with async_timeout.timeout(10):
        async with session.post(url, data=msg) as response:
            return await response.text()

async def main():
    async with aiohttp.ClientSession() as session:
        f = open('../users/data/users.jsonl', 'r')
        msg = get_json_string(f)
        while not msg is None and not msg == '':
            await post(session, 'http://localhost:8080/users', msg)
            msg = get_json_string(f)

        # f = open('../users/data/connections.jsonl', 'r')
        # msg = get_json_string(f)
        # while not msg is None and not msg == '':
        #     await post(session, 'http://localhost:8080/connections', msg)
        #     msg = get_json_string(f)



loop = asyncio.get_event_loop()
loop.run_until_complete(main())
