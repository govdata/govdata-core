before="$(date +%s)"
curl "http://ec2-67-202-31-123.compute-1.amazonaws.com/get?callback=jsonp1285439212721&q=%7B%22query%22%3A%5B%7B%22action%22%3A%22find%22%2C%22args%22%3A%5B%7B%22Industry%22%3A%22Arts%2C+Entertainment%2C+and+Recreation%22%7D%5D%7D%2C%7B%22action%22%3A%22skip%22%2C%22args%22%3A%5B0%5D%7D%2C%7B%22action%22%3A%22limit%22%2C%22args%22%3A%5B30%5D%7D%5D%2C%22collection%22%3A%22BLS_sm%22%7D" > /dev/null
after="$(date +%s)"
elapsed_seconds="$(expr $after - $before)"
echo Elapsed time for code block: $elapsed_seconds
