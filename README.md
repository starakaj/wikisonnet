# wikisonnet
Automatically generate sonnets from wikipedia

Random notes about heroku:

First you need to move the database to the heroku application. So go to the directory of the app and 
heroku addons:create heroku-postgresql:hobby-basic

Make a db dump
pg_dump -Fc mydb > db.dump

Next upload a .dump of the database to amazon s3. Python with tinys3 is great

Finally, 
heroku pg:backups restore 'https://s3.amazonaws.com/me/items/3H0q/mydb.dump' DATABASE_URL

Where obviously the amazonaws link should be replaced with an actual link