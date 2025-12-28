import requests

sess = requests.Session()
# try login with common default user
login_data = {'username':'julio','password':'12345'}
resp = sess.post('http://127.0.0.1:5000/login', data=login_data, allow_redirects=True)
print('Login status:', resp.status_code)
r = sess.get('http://127.0.0.1:5000/estoque/transferir')
print('Transferir status:', r.status_code)
print('Body start:\n', r.text[:1000])
