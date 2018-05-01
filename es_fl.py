from flask import Flask, request
import json
from es import ES
app = Flask(__name__)


@app.route('/')
def home():
    return ('Go to /search with POST method with payload: '
            '{"query": "some_query", "files_and_folders": "0"}.    '
            'files_and_folders values: 0 for files, 1 for files and folders')


@app.route('/search', methods=['POST'])
def search():
    # accepts json payload with "query" key and "files_and_folders"
    # query is mandatory, files_and_folders default value is 1
    es = ES()
    content = request.get_data()
    post_input = json.loads(content)

    try:
        query = post_input['query']
    except KeyError:
        return 'No search query!'

    try:
        files_and_folders = int(post_input['files_and_folders'])
    except (KeyError, ValueError):
        files_and_folders = 1
    
    if files_and_folders:
        output = es.query_all(query)
    else:
        output = es.query_files(query)
    return output