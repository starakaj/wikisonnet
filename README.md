# wikisonnet
Automatically generate sonnets from wikipedia

scraper/src contains files needed for the scraping phase, while server contains files needed to serve up poems

scraper also needs some of the files in server. I haven't figured out how to make this work yet.
Because of this, scraper is actually broken for now

To run the server locally...
cd src
virtualenv /tmp/wikisonnet
source /tmp/wikisonnet/bin/activate
pip install -r requirements
pynt start_server

You'll also need a file called dbconfig.yml in your server directory. It should look like:

name:
  database: <database>
  host: <host>
  user: <user>
  password: <password>
