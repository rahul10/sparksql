#!/usr/bin/python
import cgi,cgitb,webbrowser,os			#importing modules
import sys,commands

print "Content-type:text/html"
print ""					#header

data=cgi.FieldStorage()
query=data.getvalue('q')

file1=open("/var/www/cgi-bin/hiveql/q.txt","w")
file1.write(query)
file1.close()

cmd='sudo /spark/bin/spark-submit /var/www/html/retexe.py  >/var/www/cgi-bin/hiveql/logs/output.txt' 
g=commands.getoutput(cmd);
file1=open("/var/www/cgi-bin/hiveql/logs/output.txt","r")
print "<html><p>"+g+"</p></html>"
