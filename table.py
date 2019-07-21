#!/usr/bin/python
import cgi,cgitb,webbrowser,os			#importing modules
import sys,commands

print "Content-type:text/html"
print ""					#header

data=cgi.FieldStorage()
table=data.getvalue('t')

cmd='sudo /spark/bin/spark-submit /var/www/html/ret.py '+str(table)+'  >/var/www/cgi-bin/hiveql/logs/log.txt' 
g=commands.getoutput(cmd);
#print "<html><p>"+g+"</p></html>"
file1=open("/var/www/cgi-bin/hiveql/logs/log.txt","r")
print "<html><p>"+file1.read()+"</p></html>"

x='''<html><h2>Enter Query : </h2><br>
<form action="/cgi-bin/hiveql/query.py" >
<input type="text" name="q" size="100">
<input type="submit" value="Submit">
</form>
</html>'''

print x
