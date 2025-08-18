## About

This is a [golang](https://go.dev/) implementation of the [Kademlia](https://en.wikipedia.org/wiki/Kademlia) [DHT](https://en.wikipedia.org/wiki/Distributed_hash_table) protocol. It forms a Peer-to-Peer overlay network to support building fully decentralized features for your next-gen data intensive applications. 

This library is currently used in my recent projects, I found it to be pretty stable, but its APIs have not been finalized yet.

## Features

* clean kademlia implementation, nothing to do with bittorrent's [Mainline DHT](https://en.wikipedia.org/wiki/Mainline_DHT)
* pure Golang, no cgo
* a simple in-memory key-value store with TTL support 

Being able to defend [sybil attacks](https://en.wikipedia.org/wiki/Sybil_attack) is not planned, users need to ensure only participants running trusted code are allowed to join the DHT.

## How to Use

See struct DHT's godoc for details, tests in dht\_test.go demonstrate how to use the library. 

## The Experiment

This library is a little bit special in the sense that other than the selected LLM (OpenAI [GPT-4o](https://en.wikipedia.org/wiki/GPT-4o)) no other references (the original paper, search engine, online discussion forum or blog articles etc.) is ever consulted during the development in mid 2024. This experiment helped to understand how much a SOTA LLM can assist during software development by just answering questions. My overall experience strongly indicates that SOTA LLMs like OpenAI GPT-4o is already capable of correctly presenting and explaining the full details of a public and complex network protocol like Kadelmia for the purposes of implementing the concerned protocol.

The development of this project didn't involve any vibe coding tools. 

## License

This library follows a dual licensing model - 

* it is licensed under the 2-clause BSD license if you have written evidence showing that you are a licensee of github.com/lni/pothos
* otherwise, it is licensed under the GPL-2 license 
