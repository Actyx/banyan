trap 'kill $(jobs -p)' EXIT
ssh -L 5001:localhost:5001 klaehn.org &
ipfs --api /ip4/127.0.0.1/tcp/5001 add -r ./docs/pictures
