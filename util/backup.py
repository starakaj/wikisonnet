import os
import yaml
import time

### Config ###
diable_host = "diable@Diable.local"
diable_dir = "/Users/diable/Documents/wikisonnet/"
s3_bucket = "wikisonnet"
### Config ###

datetime = time.strftime('%m%d%Y-%H%M%S')

try:
    spath = os.path.dirname(os.path.realpath(__file__))
    f = open(spath + '/../server/dbconfig.yml', 'r')
    databases = yaml.load(f)
except Exception as e:
    print str(e)

dbconfig = databases['local']

oname = "wikisonnet-{}.sql".format(datetime)
print "Backing up database locally to {}".format(oname)
dumpcommand = "/bin/bash -c '/usr/local/mysql/bin/mysqldump --databases {} --master-data=2  --single-transaction --order-by-primary -uroot -p -r {}'".format(dbconfig["database"], oname)
os.system(dumpcommand)
print "Local backup complete\n"

print "Copying database to {}".format(diable_host)
scpcommand = "/bin/bash -c 'scp {0} {1}:{2}{0}'".format(oname, diable_host, diable_dir)
os.system(scpcommand)
print "Finished copying to {}\n".format(diable_host)

print "Compressing database dump"
gzipname = oname + ".gz"
gzipcommand = "/bin/bash -c \"ssh {} 'gzip {}{}'\"".format(diable_host, diable_dir, oname)
os.system(gzipcommand)
print "Finished Compressing database dump\n"

print "Uploading to S3"
s3command = "/bin/bash -c \"ssh {} '/usr/local/bin/aws s3 cp {}{} s3://{} --grants read=uri=http://acs.amazonaws.com/groups/global/AllUsers full=emailaddress=starakajian@gmail.com'\"".format(diable_host, diable_dir, gzipname, s3_bucket)
os.system(s3command)
print "Finished s3 upload\n"

s3url = "https://s3.amazonaws.com/" + s3_bucket + "/" + gzipname
tellslack = """curl -X POST --data-urlencode 'payload={{"channel": "#general", "username": "backitup", "text": "Someone just uploaded a new backup to S3 {}", "icon_emoji": ":truck:"}}' https://hooks.slack.com/services/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""".format(s3url)
os.system(tellslack)

print "Backup complete!"
