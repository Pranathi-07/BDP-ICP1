import socket
import sys
import requests
import requests_oauthlib
import json
# api keys 
ACCESS_TOKEN = '3021476203-UcJfdddq2NUVFffWi8gtDYBMX99T4yqRWZo5GyO'
ACCESS_SECRET = 'wnsxz635ODphYUuUj71tLxurUbGVQFQoT2O3DGDOkINf2'
CONSUMER_KEY = 'Xgd8yHOAnEZdXgpJRxtrm8owM'
CONSUMER_SECRET = '94ULvxxQwVe1WuMewuGKhh1e6W0LVZrMqhTGS9CuL6pGzEgs5Y'
# passing the keys using requests_oauthlib
my_auth = requests_oauthlib.OAuth1(CONSUMER_KEY, CONSUMER_SECRET,ACCESS_TOKEN, ACCESS_SECRET)
# getting the tweets from the stream
def getting_tweets():
	url = 'https://stream.twitter.com/1.1/statuses/filter.json'
	query_data = [('language', 'en'), ('locations', '-130,-20,100,50'),('track','#')]
	query_url = url + '?' + '&'.join([str(t[0]) + '=' + str(t[1]) for t in query_data])
	response = requests.get(query_url, auth=my_auth, stream=True)
	print(query_url, response)
	return response
# sending the tweets to spark from the response
def tweets_spark(http_resp, tcp_connection):
    for line in http_resp.iter_lines():
        try:
            tweet = json.loads(line)
            text = tweet['text']
            print("Tweet Text: " + text)
            print ("------------------------------------------")
            tcp_connection.send(text + '\n')
        except:
            e = sys.exc_info()[0]
            print("Error: %s" % e)
IP = "localhost"
PORT_NO = 9009
conn = None
# initializing the socket and passing the ip and port
soc = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
soc.bind((IP, PORT_NO))
soc.listen(1)
print("Waiting for TCP connection...")
# getting the connection and address
conn, addr = soc.accept()
print("Connected... Starting getting tweets.")
res = getting_tweets()
# passing the response and connection
tweets_spark(res, conn)