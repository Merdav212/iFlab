from flask import Flask, request, jsonify
from PythonScript import main
import threading
import logging
from flask.logging import default_handler



app = Flask(__name__)

app.logger.removeHandler(default_handler)
# Configure the logging for your Flask app
logging.basicConfig(level=logging.DEBUG)  # Set the desired logging level

def run_main_in_thread(start_date, end_date, database_name):
    with app.app_context():
        try:
            main(start_date, end_date, database_name)
            app.logger.info("Data download initiated: %s, %s, %s", start_date, end_date, database_name)
            return jsonify({
                "message": "Data download initiated.",
                "start_date": start_date,
                "end_date": end_date,
                "database_name": database_name
            })
        except Exception as e:
            app.logger.error("An error occurred during main: %s", str(e))
            return jsonify({
                "error": f"An error occurred during main: {str(e)}",
                "start_date": start_date,
                "end_date": end_date,
                "database_name": database_name
            })

@app.route('/your-endpoint', methods=['POST'])
def your_endpoint():
    with app.app_context():
        try:
            start_date = request.form.get('start_date')
            end_date = request.form.get('end_date')
            database_name = request.form.get('database_name')

            # Create a thread to run the data download
            thread = threading.Thread(target=run_main_in_thread, args=(start_date, end_date, database_name))
            thread.start()

            app.logger.info("Data download initiated: %s, %s, %s", start_date, end_date, database_name)

            return jsonify({
                "message": "Data download initiated.",
                "start_date": start_date,
                "end_date": end_date,
                "database_name": database_name
            })
        except Exception as e:
            app.logger.error("An error occurred during data download: %s", str(e))
            return jsonify({
                "error": f"An error occurred during data download: {str(e)}",
                "start_date": start_date,
                "end_date": end_date,
                "database_name": database_name
            })

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5002, use_reloader=True, debug=False)
