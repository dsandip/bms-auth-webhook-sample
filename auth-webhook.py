import os
import logging
from flask import Flask
from flask import request, jsonify, abort
from urllib.parse import urlparse
import psycopg2


app = Flask(__name__)

# parse connection params using the env variable for PG connection
def get_database_connection_params():
    
    parsed_connection_string = urlparse(str(os.environ.get("DATABASE_URL")))
    # netloc='username:password@host:port
    # path ='/database'
    connection_params = {}
    connection_params["database"] = parsed_connection_string.path.replace("/","")
    connection_params["user"] = parsed_connection_string.netloc.split(":")[0]
    connection_params["password"] = parsed_connection_string.netloc.split(":")[1].split("@")[0]
    connection_params["host"] = parsed_connection_string.netloc.split(":")[1].split("@")[1]
    connection_params["port"] = parsed_connection_string.netloc.split(":")[2]

    return connection_params


def get_details_for_token(token):
    
    # connect to the database to verify token being legit
    connection_params = get_database_connection_params()
    con = psycopg2.connect(database= connection_params["database"], user=connection_params["user"], password=connection_params["password"], host=connection_params["host"], port=connection_params["port"])
    cur = con.cursor()
    
    # Check if token exists in our token's table
    cur.execute("SELECT id, token, consumer_id,consumer_role, issued_at, valid_till from consumer_tokens where token =\'" + token + "\'")
    rows = cur.fetchall()
    
    # define empty dict
    hasura_session_variables = {}

    # token exists is our query returned any resultset
    if len(rows) > 0:
        
        # the "token" column has a unique constraint
        hasura_session_variables["X-Hasura-Role"] = rows[0][3]
        hasura_session_variables["X-Hasura-User-Id"] = str(rows[0][2])
    
    con.close()
    return hasura_session_variables


@app.route('/')
def hello():
    return 'webhook is running'


@app.route('/auth-webhook')
def auth_webhook():
    
    # get the auth token from Authorization header
    # similarly you can access all headers sent in the request. Hasura forwards
    # all request headers to the webhook
    token = request.headers.get('Authorization')
    
    if token is None:
        # reject the graphql request
        return abort(401)
    
    # get the role and other session variables for this token from the database
    hasura_session_variables = get_details_for_token(token)

    # our function returns an empty dict if the token is invalid
    if len(hasura_session_variables.keys()) > 0:
        # allow the graphql request with session variables
        return jsonify(hasura_session_variables)
    else:
        # reject the graphql request
        return abort(401)