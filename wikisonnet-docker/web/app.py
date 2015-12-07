# app.py


from flask import Flask
from flask import request, render_template
from config import BaseConfig
import dbconnect, dbreader

app = Flask(__name__)
app.config.from_object(BaseConfig)


@app.route('/', methods=['GET'])
def index():
    return render_template('index.html')

# add a rule that actually talks to the database
@app.route('/api/v2/random', methods=['GET'])
def rdm():
    dbconn = dbconnect.MySQLDatabaseConnection(app.config['DB_NAME'],
                                                app.config['DB_USER'],
                                                app.config['DB_HOST'],
                                                app.config['DB_PASS'])
    pageID = dbreader.randomIndexedPage(dbconn)
    pageTitle = dbreader.pageTitleForPageID(dbconn, pageID)
    return render_template('random.html', nonsense=pageTitle)


if __name__ == '__main__':
    app.run()
