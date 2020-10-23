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
    index = request.args.get('index')
    if num_recs is not None:
        if category is not None:
            query = "MATCH (project:Project)-[:IS_CAT]->(category:Category {display_name: '" + category + "'}), (user:User)-[:MANAGE]->(project) RETURN project.display_name as display_name, project.status as status, project.created_ts as created_ts, project.description as description, user.f_name as f_name, user.l_name as l_name SKIP " + index + " LIMIT " + num_recs
        else:
            query = "MATCH (project:Project), (user:User)-[:MANAGE]->(project) RETURN project.display_name as display_name, project.status as status, project.created_ts as created_ts, project.description as description, user.f_name as f_name, user.l_name as l_name SKIP " + index + " LIMIT " + num_recs
        r = exe_query(query)
        response = parse_get(r)
    else:
        response = { 'status_code': 'Number of records are missing' }

    return response

##Creates a new user
@app.route('/user', methods=['PUT'])
def create_user():
    user = request.get_json()

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
        zipcode = user['zipcode']
        province = user['province']
        avatar = user['avatar']
        last_logged_in_ts = user['last_logged_in_ts']
        member_since_ts = user['member_since_ts']

        if zipcode is None:
            zipcode = ''
        if username or hashed_pass or salt or f_name or l_name or email or description or dob or city or province or last_logged_in_ts or member_since_ts or avatar is not None:
            query = "MATCH (country:Country {id: '51438616-9ee6-4dd8-bc57-6a65c4e90bdf'}) MERGE (place:Place {city: '" + city + "', province_dn: '" + province + "', zipcode: '" + zipcode + "'})-[:LOCATED_IN]->(country) ON CREATE SET place.zipcode = '" + zipcode + "', place.id = apoc.create.uuid() CREATE (user:User {id: apoc.create.uuid(), username: '" + username + "', password: '" + hashed_pass + "', salt: '" + salt + "', f_name: '" + f_name + "', l_name: '" + l_name + "', status: 'A', email: '" + email + "', description: '" + description + "', dob: '"+ dob + "', last_logged_in_ts: '" + last_logged_in_ts +"', member_since_ts: '" + member_since_ts +"'})-[resides:RESIDE_IN]->(place), (user)-[has:HAS_AVATAR]->(avatar:Avatar {id: apoc.create.uuid(), url: '" + avatar + "', display_name: '" + username + "'}) RETURN user"
            r = exe_query(query)
            response = parse_put(r)
        else:
            response = { 'status_code': 'A parameter is missing' }
    else:
        response = { 'status_code': 'Bad request' }

    return response

##Gets data for user authorization
@app.route('/creds/<username>', methods=['GET'])
def get_auth(username = None):
    query = "MATCH (user:User {username: '" + username + "'}) RETURN user.password as password, user.salt as salt"
    r = exe_query(query)
    return r

##Gets data necessary for the user profile
@app.route('/user/<user_id>/profile', methods=['GET'])
def get_user_profile(user_id = None):
    query = "MATCH (user:User {id: '" + user_id + "'})-[resides:RESIDE_IN]->(place), (user)-[has:HAS_AVATAR]->(avatar:Avatar) RETURN user.username as username, user.f_name as f_name, user.l_name as l_name, user.description as description, user.email as email, user.status as status, place.province_dn as province, avatar.url as avatar"
    r = exe_query(query)
    return r

##Submits data to update the user profile
@app.route('/user/<user_id>/profile', methods=['PUT'])
def update_user_profile(user_id = None):
    user = request.get_json()
    profile_data_query = ''

    if user_id is not None:
        if 'city' in user:
            city = user['city']
        if 'province' in user:
            province = user['province']
        if 'zipcode' in user:
            zipcode = user['zipcode']

        query = "MATCH (user:User {id: '" + user_id + "'})-[has:HAS_AVATAR]->(avatar:Avatar), (country:Country {id: '51438616-9ee6-4dd8-bc57-6a65c4e90bdf'}) MERGE (updated_place:Place {city: '" + city + "', province_dn: '" + province + "', zipcode: '" + zipcode + "'})-[located:LOCATED_IN]->(country) ON CREATE SET updated_place.id = apoc.create.uuid() MERGE (user)-[resides:RESIDE_IN]->(updated_place) "
    else:
        return { 'status_code': 'Bad request' }

    if 'username' in user:
        username = user['username']
        profile_data_query += "user.username = '" + username + "', "
    if 'hashed_pass' in user:    
        hashed_pass = user['hashed_pass']
        profile_data_query += "user.hashed_pass = '" + hashed_pass + "', "
    if 'salt' in user:
        salt = user['salt']
        profile_data_query += "user.salt = '" + salt + "', "
    if 'f_name' in user:
        f_name = user['f_name']
        profile_data_query += "user.f_name = '" + f_name + "', "
    if 'l_name' in user:
        l_name = user['l_name']
        profile_data_query += "user.l_name = '" + l_name + "', "
    if 'email' in user:
        email = user['email']
        profile_data_query += "user.email = '" + email + "', "
    if 'description' in user:
        description = user['description']
        profile_data_query += "user.description = '" + description + "', "
    if 'dob' in user:
        dob = user['dob']
        profile_data_query += "user.dob = '" + dob + "', "
    if 'avatar' in user:
        avatar = user['avatar']
        profile_data_query += "avatar.url = '" + avatar + "' "
    
    place_data_query = "WITH user OPTIONAL MATCH (user)-[resides:RESIDE_IN]->(place:Place) WHERE NOT place.zipcode = '" + zipcode + "' DELETE resides "

    query += "ON MATCH SET " + profile_data_query
    query += "ON CREATE SET " + profile_data_query + place_data_query
    query += "RETURN user.id as id"

    r = exe_query(query)
    return r

#Change User Status
@app.route('/user/<user_id>/profile/status', methods=['PUT'])
def update_user_status(user_id = None):
    status_json = request.get_json()
    
    if user_id is not None:
        if 'status' in status_json:
            status = status_json['status']
            query = "MATCH (user:User {id: '" + user_id + "'}) SET user.status = '" + status + "' RETURN user.status as status"
            r = exe_query(query)
        else:
            r = {'status_code': 'Missing status'}
    else:
        r = {'status_code': '400 Bad Request'}

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
    except neo4j.exceptions.ServiceUnavailable as err:
        print(err)
        r = { 'status_code': 'ERR_DB_SRVC_UNAVAIL' }
    except neo4j.exceptions.SessionExpired as err:
        print(err)
        r = { 'status_code': 'ERR_DB_SESSION_UNAVAIL' }
    except neo4j.exceptions.ConstraintError as err:
        print(err.message)
        r = { 'status_code': 'ERR_DB_CONSTRAINT_FAIL'}
    # except:
    #     if CONFIG['neo4j']['debug_enabled']['default'] == True:
    #         traceback.print_exc()
    #     r = { 'status_code': 'ERR_DB_UNKNOWN'}

    return r

def parse_get(r):
    num_results = len(r['results'])
    r.update({'num_results': num_results})
    return r

def parse_put(r):
    status_code = r['status_code']
    response = {'status_code': status_code}
    return response