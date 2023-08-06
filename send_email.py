from flask import Flask, request, abort
import json
import subprocess
import os

app = Flask(__name__)

@app.route('/', methods=['POST'])
def handle_pubsub_message():
    try:
        envelope = json.loads(request.data.decode('utf-8'))
        payload = envelope['message']['data']

        subprocess.run(['python', 'send_results_email.py'])

        return ('', 204)
    except Exception as e:
        # Log the error
        print(e)
        # Return an error response
        abort(500)

if __name__ == "__main__":
    app.run(debug=True, host='0.0.0.0', port=int(os.environ.get('PORT', 8080)))
