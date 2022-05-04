* * * * * /bin/bash -c /home/ubuntu/kiran/systemload.sh
if [ $(ps -ef |grep python3  | wc -l) = "1" ]; then echo "Subject : python process down, login now" | sendmail kiran.shashi@intellibus.com; fi