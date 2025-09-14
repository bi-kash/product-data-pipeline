import iop
url = 'https://api-sg.aliexpress.com/sync'
appkey = '519268'
appSecret = 'FU2YX7z6ZwYeOhlIRv8sdjFocRXrrsAM'
# UUID = 'uuid'
# CODE = '3_519268_kw0lMTNuVEdml9625d22gueN5376'
client = iop.IopClient('https://api-sg.aliexpress.com/rest', appkey ,appSecret)
request = iop.IopRequest('/auth/token/create')
request.add_api_param('code', CODE)

response = client.execute(request)
print(response.type)
print(response.body)

client = iop.IopClient(url, appkey ,appSecret)
request = iop.IopRequest('aliexpress.ds.text.search')
request.add_api_param('session', '50000901030rK4qYZ0o1iqRhTuhYbdP3szUChHPKxi1328e634zsmfxjtPsnQvWqHAfB')
request.add_api_param('keyWord', 'gold')
request.add_api_param('local', 'en_US')
request.add_api_param('countryCode', 'US')

request.add_api_param('currency', 'USD')


response = client.execute(request)
import json

# assuming response.body is a dict
with open("output.json", "w") as f:
    f.write(json.dumps(response.body, indent=4))  # convert dict to formatted string
print(response.type)
print(response.body)