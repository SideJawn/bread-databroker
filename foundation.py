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

#Loads all category names
@app.route('/categories', methods=['GET'])
def get_categories():
    query="MATCH (category:Category) RETURN category.display_name as display_name, category.id as id"
    r = exe_query(query)
    return r

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
            query = "MATCH (country:Country {id: '51438616-9ee6-4dd8-bc57-6a65c4e90bdf'}) MERGE (place:Place {city: '" + city + "', province_dn: '" + province + "', zipcode: '" + zipcode + "'})-[:LOCATED_IN]->(country) ON CREATE SET place.zipcode = '" + zipcode + "', place.id = apoc.create.uuid() CREATE (user:User {id: apoc.create.uuid(), username: '" + username + "', password: '" + hashed_pass + "', salt: '" + salt + "', f_name: '" + f_name + "', l_name: '" + l_name + "', status: 'A', email: '" + email + "', description: '" + description + "', dob: '"+ dob + "', last_logged_in_ts: '" + last_logged_in_ts +"', member_since_ts: '" + member_since_ts +"'})-[resides:RESIDE_IN]->(place), (user)-[has:HAS_AVATAR]->(avatar:Avatar {id: apoc.create.uuid(), url: '" + avatar + "', display_name: '" + username + "'}) RETURN NULL"
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

#Searches for basic user info by email
@app.route('/user/<user_email>/basic', methods=['GET'])
def get_user_basic(user_email = None):
    query = "MATCH (user:User {email: '" + user_email + "'})-[has:HAS_AVATAR]->(avatar:Avatar) RETURN user.f_name as f_name, user.l_name as l_name, user.username as username, avatar.url as avatar"
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

#Create a project
@app.route('/project', methods=['PUT'])
def create_project():
    project = request.get_json()
    add_existing_contributors = False
    add_recruits = False

    if 'user_id' in project and project['user_id'] is not None:
        user_id = project['user_id']
    if 'display_name' in project and project['display_name'] is not None:
        display_name = project['display_name']
    if 'project_description' in project and project['project_description'] is not None:
        project_description = project['project_description']
    if 'created_ts' in project and project['created_ts'] is not None:
        created_ts = project['created_ts'] 
    if 'deadline_ts' in project and project['deadline_ts'] is not None:
        deadline_ts = project['deadline_ts']
    if 'category_id' in project and project['category_id'] is not None:
        category_id = project['category_id']
    if 'existing_contributors' in project and project['existing_contributors'] is not None:
        existing_contributors = project['existing_contributors']
        add_existing_contributors = True
    if 'recruiting_roles' in project and project['recruiting_roles'] is not None:
        recruiting_roles = project['recruiting_roles']
        add_recruits = True

    query = "MATCH (user:User {id: '" + user_id + "'}), (category:Category {id: '" + category_id + "'}) CREATE (user)-[manages:MANAGE {role: 'OWNER'}]->(project:Project {id: apoc.create.uuid(), display_name: '" + display_name + "', status: 'N', is_flagged: 'F', flagged_reason: null, description: '" + project_description + "', created_ts: '" + created_ts + "', deadline_ts: '" + deadline_ts + "'})-[is:IS_CAT]->(category)"
        
    if add_recruits is True:
        for recruit in recruiting_roles:
            if 'role' in recruit and recruit['role'] is not None:
                recruit_role = recruit['role']
            if 'description' in recruit and recruit['description'] is not None:
                recruit_description = recruit['description']
            
            query += ", (project)-[:NEED_CONTRIBUTOR]->(:Contributor {id: apoc.create.uuid(), role: '" + recruit_role + "', description: '" + recruit_description + "'}) "

    if add_existing_contributors is True:
        for contributor in existing_contributors:
            needed_by = ''
            if 'role' in contributor and contributor['role'] is not None:
                contributor_role = contributor['role']
            if 'f_name' in contributor and contributor['f_name'] is not None:
                f_name = contributor['f_name']
            if 'l_name' in contributor and contributor['l_name'] is not None:
                l_name = contributor['l_name']
            if 'description' in contributor and contributor['description'] is not None:
                role_description = contributor['description']
            if 'user_id' in contributor and contributor['user_id'] is not None:
                contributor_id = contributor['user_id']
            if 'needed_by' in contributor and contributor['needed_by'] is not None:
                needed_by = contributor['needed_by']
            
            if 'contributor_id' in locals() and contributor_id is not None:
                query += "WITH project MATCH (user:User {id: '" + contributor_id + "'}) CREATE (project)-[:NEED_CONTRIBUTOR {status: 'N', needed_by: '" + needed_by + "', user_id: '" + contributor_id + "'}]->(:Contributor {id: apoc.create.uuid(), role: '" + contributor_role + "', description: '" + role_description + "'})<-[:IS_CONTRIBUTOR]-(user) "
            else:
                query += ", (project)-[:NEED_CONTRIBUTOR]->(:Contributor {id: apoc.create.uuid(), role: '" + contributor_role + "', display_name: '" + f_name + ' ' + l_name + "',description: '" + role_description + "'}) "
    
    query += "RETURN NULL"
    r = exe_query(query)
    return r

#Updates project general info
@app.route('/project/<project_id>', methods=['PUT'])
def update_project(project_id = None):
    project = request.get_json()

    if project_id is not None:
        query = "MATCH (project:Project {id: '" + project_id + "'})"
        category_id_exists = False
        if 'category_id' in project and project['category_id'] is not None:
            category_id_exists = True
            category_id = project['category_id']
            query += "-[is_cat:IS_CAT]->(current_category:Category), (new_category:Category {id: '" + category_id + "'}) MERGE (project)-[:IS_CAT]->(new_category) "
        else:
            query += " "
        if 'display_name' in project and project['display_name'] is not None:
            display_name = project['display_name']
            query += "SET project.display_name = '" + display_name + "' "
        if 'project_description' in project and project['project_description'] is not None:
            project_description = project['project_description']
            query += "SET project.description = '" + project_description + "' "
        if 'deadline_ts' in project and project['deadline_ts'] is not None:
            deadline_ts = project['deadline_ts']
            query += "SET project.deadline_ts = '" + deadline_ts + "' "
        if 'status' in project and project['status'] is not None:
            status = project['status']
            query += "SET project.status = '" + status + "' "
        if 'user_id' in project and project['user_id'] is not None:
            user_id = project['user_id']
            query += "with project"
            if category_id_exists is True:
                query += ", is_cat"
            query += " Match (old_owner:User)-[manage:MANAGE]->(project), (new_owner:User {id: '" + user_id + "'}) CREATE (new_owner)-[:MANAGE]->(project) DELETE manage"
            if category_id_exists is True:
                query += ", is_cat"
        if category_id_exists is True and 'user_id' not in locals():
            query += "DELETE is_cat"
        
        query += " RETURN NULL"
        response = exe_query(query)
    else:
        return { 'status_code': 'Bad request' }
    
    return response

#Updates project contributors
@app.route('/project/<project_id>/contributors', methods=['PUT'])
def update_project_contributors(project_id = None):
    updates = request.get_json()
    add_contributors = None
    remove_contributors = None
    add_response = {}
    remove_response = {}

    if 'add_contributors' in updates and updates['add_contributors'] is not None:
        add_contributors = updates['add_contributors']
        add_response = add_project_contributors(project_id, add_contributors)
    if 'remove_contributors' in updates and 'remove_contributors' is not None:
        remove_contributors = updates['remove_contributors']
        remove_response = remove_project_contributors(project_id, remove_contributors)
    
    if ('status_code' in add_response and add_response['status_code'] == 'OK') or ('status_code' in remove_response and remove_response['status_code'] == 'OK'):
        return {'status_code' : 'OK'}
    else:
        return {'status_code' : 'Internal Error'}

def add_project_contributors(project_id, contributors):
    add_existing_contributors = False
    add_recruits = False

    if project_id is not None and contributors is not None:
        query = "MATCH (project:Project {id: '" + project_id + "'}) "
        if 'existing_contributors' in contributors and contributors['existing_contributors'] is not None:
            existing_contributors = contributors['existing_contributors']
            add_existing_contributors = True
        if 'recruiting_roles' in contributors and contributors['recruiting_roles'] is not None:
            recruiting_roles = contributors['recruiting_roles']
            add_recruits = True
    
        if add_recruits is True:
            for recruit in recruiting_roles:
                needed_by = ''
                if 'needed_by' in recruit and recruit['needed_by'] is not None:
                    needed_by = recruit['needed_by']
                if 'contributor_id' in recruit and recruit['contributor_id'] is not None:
                    default_contributor_id = recruit['contributor_id']
                    query += "WITH project MATCH (contributor:Contributor {id: '" + default_contributor_id + "'}) CREATE (project)-[:NEED_CONTRIBUTOR]->(contributor) "
                else:
                    if 'role' in recruit and recruit['role'] is not None:
                        recruit_role = recruit['role']
                    if 'description' in recruit and recruit['description'] is not None:
                        recruit_description = recruit['description']
                    query += "WITH project CREATE (project)-[:NEED_CONTRIBUTOR {needed_by: '" + needed_by + "'}]->(:Contributor {id: apoc.create.uuid(), role: '" + recruit_role + "', description: '" + recruit_description + "'}) "

        if add_existing_contributors is True:
            for contributor in existing_contributors:
                needed_by = ''
                existing_contributor_template = False
                if 'role' in contributor and contributor['role'] is not None:
                    contributor_role = contributor['role']
                if 'description' in contributor and contributor['description'] is not None:
                    role_description = contributor['description']
                if 'user_id' in contributor and contributor['user_id'] is not None:
                    user_id = contributor['user_id']
                if 'f_name' in contributor and contributor['f_name'] is not None:
                    f_name = contributor['f_name']
                if 'l_name' in contributor and contributor['l_name'] is not None:
                    l_name = contributor['l_name']
                if 'email' in contributor and contributor['email'] is not None:
                    email = contributor['email']
                if 'needed_by' in contributor and contributor['needed_by'] is not None:
                    needed_by = contributor['needed_by']
                if 'contributor_id' in contributor and contributor['contributor_id'] is not None:
                    default_contributor_id = contributor['contributor_id']
                    existing_contributor_template = True

                if 'user_id' in locals() and user_id is not None:
                    if existing_contributor_template is True:
                        query += "WITH project MATCH (user:User {id: '" + user_id + "'}), (contributor:Contributor {id: '" + default_contributor_id + "'}) CREATE (project)-[:NEED_CONTRIBUTOR {status: 'N', needed_by: '" + needed_by + "', user_id: '" + user_id + "'}]->(contributor)<-[:IS_CONTRIBUTOR]-(user) "
                    else:
                        query += "WITH project MATCH (user:User {id: '" + user_id + "'}) CREATE (project)-[:NEED_CONTRIBUTOR {status: 'N', needed_by: '" + needed_by + "', user_id: '" + user_id + "'}]->(:Contributor {id: apoc.create.uuid(), role: '" + contributor_role + "', description: '" + role_description + "'})<-[:IS_CONTRIBUTOR]-(user) "
                elif 'f_name' in locals() and f_name is not None and 'l_name' in locals() and l_name is not None and 'email' in locals() and email is not None:
                    if existing_contributor_template is True:
                        query += "WITH project MATCH (contributor:Contributor {id: '" + default_contributor_id + "'}) CREATE (project)-[:NEED_CONTRIBUTOR {status: 'N', needed_by: '" + needed_by + "', email: '" + email + "'}]->(contributor)<-[:IS_CONTRIBUTOR]-(user:User {id: apoc.create.uuid(), f_name: '" + f_name + "', l_name: '" + l_name + "', email: '" + email + "', status: 'T'}) "
                    else:
                        query += "WITH project CREATE (project)-[:NEED_CONTRIBUTOR {status: 'N', needed_by: '" + needed_by + "', email: '" + email + "'}]->(:Contributor {id: apoc.create.uuid(), role: '" + contributor_role + "', description: '" + role_description + "'})<-[:IS_CONTRIBUTOR]-(user:User {id: apoc.create.uuid(), f_name: '" + f_name + "', l_name: '" + l_name + "', email: '" + email + "', status: 'T'}) "
                else:
                    return { 'status_code': 'Bad request' }   
        
        if add_recruits is False and add_existing_contributors is False:
            return { 'status_code': 'Bad request' }
        
        query += "RETURN NULL"
        response = exe_query(query)
    else:
        return { 'status_code': 'Bad request' }

    return response

def remove_project_contributors(project_id, contributors):
    if project_id is not None and contributors is not None:
        query = "MATCH (project:Project {id: '" + project_id + "'})-[needs:NEED_CONTRIBUTOR]->(contributor:Contributor) WHERE contributor.id IN ["

        for count, contributor in enumerate(contributors):
            if 'id' in contributor and contributor['id'] is not None:
                query += "'" + contributor['id'] + "'"
                if count != (len(contributors)-1):
                    query += ", "
        query += '] with needs, contributor OPTIONAL MATCH (contributor)<-[is:IS_CONTRIBUTOR]-(user:User) DELETE needs, is RETURN NULL'
        response = exe_query(query)
    else:
        return { 'status_code': 'Bad request' }
    
    return response


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
                    if 'NULL' in sorted_results[0]:
                        r = {
                            'status_code': 'OK',
                        }
                    else:
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