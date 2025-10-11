#!/bin/sh
# bin/serverledge-cli create -f func --memory "$2" --src examples/hello.py --runtime python310 --handler "hello.handler" -H "$1"
# bin/serverledge-cli create -f fib --memory "$2" --src examples/fibonacciNout.py --runtime python310 --handler "fibonacciNout.handler" -H "$1"
# bin/serverledge-cli create -f hash --memory "$2" --src examples/hash_string.py --runtime python310 --handler "hash_string.handler" -H "$1"
# bin/serverledge-cli create -f imageclass --runtime custom --custom_image grussorusso/serverledge-imageclass --memory 1024 -H "$1"

../bin/serverledge-cli create -f f1 --memory 512 --src ../examples/fibonacciNout.py --runtime python310 --handler "fibonacciNout.handler"
../bin/serverledge-cli create -f f2 --memory 512 --src ../examples/fibonacciNout.py --runtime python310 --handler "fibonacciNout.handler"
../bin/serverledge-cli create -f f3 --memory 128 --src ../examples/fibonacciNout.py --runtime python310 --handler "fibonacciNout.handler"
../bin/serverledge-cli create -f f4 --memory 1024 --src ../examples/fibonacciNout.py --runtime python310 --handler "fibonacciNout.handler"
../bin/serverledge-cli create -f f5 --memory 256 --src ../examples/fibonacciNout.py --runtime python310 --handler "fibonacciNout.handler"
