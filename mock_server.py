from aiohttp import web

def get_json_string():
  f = open('mockdata.json', 'r')
  return f.read()

async def handle_users(request):
    return web.Response(text=get_json_string(), content_type='application/json')

app = web.Application()
app.router.add_get('/users', handle_users)


web.run_app(app)
