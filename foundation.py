import sys
from sys import stdout
from flask import Flask, g, request
import json, traceback
## doc: https://neo4j.com/docs/api/python-driver/current/
import neo4j
from neo4j import GraphDatabase
import logging
app = Flask(__name__)
app.logger #  initialize logger

## Initialize neo4j driver and connect to neo4 database
try:
    uri = "neo4j://localhost:7687"
    driver = GraphDatabase.driver(uri, auth=("neo4j", "neo4jpass"))
    print('Successfullly initialized neo4j driver')
except:
    print('Error initializing neo4j driver')
    traceback.print_exc()
    sys.exit(1)

## Retrieve a db session
def get_db_session():
    if not hasattr(g, 'neo4j_db'):
        g.neo4j_db = driver.session()
    return g.neo4j_db

## Close the db connection when app shuts down.
@app.teardown_appcontext
def close_db_session(error):
    if hasattr(g, 'neo4j_db'):
        g.neo4j_db.close()

##Returns data for project cards feed
@app.route('/project', methods=['GET'])
def get_projects():
    category = request.args.get('category')
    num_recs = request.args.get('num_recs')
    if num_recs is not None:
        if category is not None:
            query = "MATCH (project:Project)-[:IS_CAT]->(category:Category {display_name: '" + category + "'}), (user:User)-[:MANAGE]->(project) RETURN project.display_name as display_name, project.status as status, project.created_ts as created_ts, project.description as description, user.f_name as f_name, user.l_name as l_name LIMIT " + num_recs
        else:
            query = "MATCH (project:Project), (user:User)-[:MANAGE]->(project) RETURN project.display_name as display_name, project.status as status, project.created_ts as created_ts, project.description as description, user.f_name as f_name, user.l_name as l_name LIMIT " + num_recs
        r = exe_query(query)
        num_projects = len(r['results'])
        r.update({'num_projects': num_projects})
    else:
        r = { 'status_code': 'Number of records are missing' }

    return r

##Creates a new user
@app.route('/user', methods=['PUT'])
def create_user():
    user = request.get_json()['data']

    if user is not None:
        username = user['username']
        hashed_pass = user['hashed_pass']
        salt = user['salt']
        f_name = user['f_name']
        l_name = user['l_name']
        email = user['email']
        description = user['description']
        dob = user['dob']
        city = user['city']
        province = user['province']
        avatar = user['avatar']
        last_logged_in_ts = user['last_logged_in_ts']
        member_since_ts = user['member_since_ts']

        if username or hashed_pass or salt or f_name or l_name or email or description or dob or city or province or last_logged_in_ts or member_since_ts or avatar is not None:
            query = "MATCH (place:Place {city: '" + city + "', province_dn: '" + province + "'}) CREATE (user:User {id: apoc.create.uuid(), username: '" + username + "', password: '" + hashed_pass + "', salt: '" + salt + "', f_name: '" + f_name + "', l_name: '" + l_name + "', status: 'A', email: '" + email + "', description: '" + description + "', dob: '"+ dob + "', last_logged_in_ts: '" + last_logged_in_ts +"', member_since_ts: '" + member_since_ts +"'})-[resides:RESIDE_IN]->(place), (user)-[has:HAS_AVATAR]->(avatar:Avatar {id: apoc.create.uuid(), url: '" + avatar + "', display_name: '" + username + "'}) RETURN *"
            r = exe_query(query)
        else:
            r = { 'status_code': 'A parameter is missing' }
    else:
        r = { 'status_code': 'Bad request' }

    return r

##Gets data necessary for user authorization
@app.route('/creds/<username>', methods=['GET'])
def get_auth(username = None):
    query = "MATCH (user:User {username: '" + username + "'}) RETURN user.password as password, user.salt as salt"
    r = exe_query(query)
    return r

def exe_query(query):
    r = {}
    try:
        db = get_db_session()
        with db as session:
            with session.begin_transaction() as tx:
                results = tx.run(query)
                ## if a record is there (by peeking), process results
                if results.peek() is not None:
                    sorted_results = [record for record in results.data()]
                    r = {
                        'status_code': 'OK',
                        'results': sorted_results
                    }
                else:
                    r = { 'status_code': 'ERR_NOT_FOUND' }
    except neo4j.exceptions.ServiceUnavailable:
        r = { 'status_code': 'ERR_DB_SRVC_UNAVAIL' }
    except neo4j.exceptions.SessionExpired:
        r = { 'status_code': 'ERR_DB_SESSION_UNAVAIL' }
    # except:
    #     if CONFIG['neo4j']['debug_enabled']['default'] == True:
    #         traceback.print_exc()
    #     r = { 'status_code': 'ERR_DB_UNKNOWN'}

    return r