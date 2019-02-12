## What/Why
I wanted to understand `zeromq` a little, so I play with it through its javascript binding.

This demo is a RPC like, the client ask for something, then register to a topic (waiting for response)
The worker register to some topics, and send back an event when its work is done, so the client can complete its own task.

## Features
 - RPC like communication, based on events
 - Retry
 - Load balancing (randomly choose a worker)

## Don't use it
This is a raw tryout, there is a lot of bugs, I don't know if this is performant.

## Test it
 - `yarn`
 - In 3 (or more different shells):
  - `node src/loadbalancer` this is the load balancer
  - `node src/invoice [timeout]` this is a worker, `timeout` is the faked processing time for a message
  - `node src/client [workers]` this is client**s**, `workers` is the number of worker you want to spawn, default is 1

## PR
Feel free to fork and open issues/PR to discuss about it!
